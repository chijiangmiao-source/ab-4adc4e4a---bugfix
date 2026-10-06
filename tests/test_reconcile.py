import hashlib
import json
import os
import tempfile
import unittest

from app import artifacts, config, reconcile, store, testsupport, worker
from app.render import render_artifact_bytes

R1 = {"rules": [
    {"field": "depth_m", "action": "redact"},
    {"field": "vessel_id", "action": "hash", "length": 10},
]}
R2 = {"rules": [{"field": "lat", "action": "redact"}]}
RECS = [{"ts": "t0", "lat": 31.23, "depth_m": 42.5, "vessel_id": "V1"}]


class ReconcileTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = "5"
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS"):
            os.environ.pop(var, None)

    def publish(self, export_id, records=RECS):
        if store.get_export(self.conn, export_id) is None:
            self.assertEqual(201, store.submit_export(self.conn, export_id, records)[0])
        fencing = store.acquire_lease(
            self.conn, worker.lease_resource(export_id), "w-test", 5)
        self.assertEqual("published", worker.process_export(self.conn, export_id, "w-test", fencing))
        store.release_lease(self.conn, worker.lease_resource(export_id), "w-test", fencing)
        return store.get_export(self.conn, export_id)


class PerSnapshotMaskingTest(ReconcileTestBase):
    def test_same_records_under_different_rules_mask_differently(self):
        store.set_rules(self.conn, R1)
        self.assertEqual(201, store.submit_export(self.conn, "E1", RECS)[0])
        store.set_rules(self.conn, R2)
        self.assertEqual(201, store.submit_export(self.conn, "E2", RECS)[0])
        e1, e2 = store.get_export(self.conn, "E1"), store.get_export(self.conn, "E2")
        self.assertNotEqual(e1["rules_digest"], e2["rules_digest"])
        self.assertEqual(e1["input_digest"], e2["input_digest"])  # identical records
        m1, m2 = json.loads(e1["masked_records"])[0], json.loads(e2["masked_records"])[0]
        self.assertEqual("***", m1["depth_m"])           # E1 follows frozen R1
        self.assertEqual(31.23, m1["lat"])
        self.assertNotEqual("V1", m1["vessel_id"])
        self.assertEqual("***", m2["lat"])               # E2 follows frozen R2
        self.assertEqual(42.5, m2["depth_m"])
        self.assertEqual("V1", m2["vessel_id"])
        # rendered artifacts differ and each names its own frozen rules digest
        a1 = json.loads(render_artifact_bytes(e1))
        a2 = json.loads(render_artifact_bytes(e2))
        self.assertEqual(a1["rules_digest"], e1["rules_digest"])
        self.assertEqual(a2["rules_digest"], e2["rules_digest"])
        self.assertNotEqual(a1["records"], a2["records"])

    def test_masking_is_derived_independently_never_borrowed(self):
        store.set_rules(self.conn, R1)
        store.submit_export(self.conn, "E1", RECS)
        store.set_rules(self.conn, R2)
        store.submit_export(self.conn, "E2", RECS)
        store.set_rules(self.conn, R1)  # cycle back to R1
        store.submit_export(self.conn, "E3", RECS)
        e1, e2, e3 = (store.get_export(self.conn, k) for k in ("E1", "E2", "E3"))
        self.assertEqual(e1["masked_records"], e3["masked_records"])  # same frozen inputs
        self.assertNotEqual(e2["masked_records"], e1["masked_records"])

    def test_poisoned_row_cannot_contaminate_a_new_submission(self):
        # A legacy-poisoned PUBLISHED row must never leak its wrong masking into
        # a later export that shares the same frozen rules snapshot.
        store.set_rules(self.conn, R1)
        store.submit_export(self.conn, "E1", RECS)
        self.publish("E1")
        self.assertIsNotNone(testsupport.poison_published(self.conn, "E1", R2))
        store.submit_export(self.conn, "E3", RECS)  # same records, same current R1
        e3 = store.get_export(self.conn, "E3")
        m3 = json.loads(e3["masked_records"])[0]
        self.assertEqual("***", m3["depth_m"])  # follows R1, not E1's poisoned R2 bytes
        self.assertEqual(31.23, m3["lat"])
        fencing = store.acquire_lease(
            self.conn, worker.lease_resource("E3"), "w-test", 5)
        self.assertEqual("published", worker.process_export(self.conn, "E3", "w-test", fencing))
        row = store.get_export(self.conn, "E3")
        with open(artifacts.published_path("E3"), "rb") as fh:
            self.assertTrue(artifacts.verify_artifact_binding(fh.read(), row))


class PublishedHealingTest(ReconcileTestBase):
    def test_poisoned_published_is_refused_then_healed_in_place(self):
        store.set_rules(self.conn, R1)
        row = self.publish("E1")
        good_digest = row["artifact_digest"]
        with open(artifacts.published_path("E1"), "rb") as fh:
            good_bytes = fh.read()

        # Legacy-bug symptom: PUBLISHED bytes follow R2 while header claims R1.
        injected = testsupport.poison_published(self.conn, "E1", R2)
        self.assertIsNotNone(injected)
        poisoned = store.get_export(self.conn, "E1")
        self.assertEqual("PUBLISHED", poisoned["stage"])
        self.assertNotEqual(good_digest, poisoned["artifact_digest"])
        with self.assertRaises(artifacts.FrozenSnapshotMismatch):
            artifacts.load_verified(poisoned)  # fail-closed download

        counts = reconcile.reconcile_all(self.conn, "reconcile")
        self.assertEqual({"healed_published": 1, "corrected_unfinished": 0}, counts)

        healed = store.get_export(self.conn, "E1")
        self.assertEqual("PUBLISHED", healed["stage"])            # no regression
        self.assertEqual(good_digest, healed["artifact_digest"])  # correct content
        data = artifacts.load_verified(healed)                    # downloadable again
        self.assertEqual(good_bytes, data)
        doc = json.loads(data)
        self.assertEqual(doc["rules_digest"], healed["rules_digest"])
        self.assertEqual("***", doc["records"][0]["depth_m"])
        self.assertEqual(31.23, doc["records"][0]["lat"])
        # frozen evidence preserved
        self.assertEqual(row["rules_digest"], healed["rules_digest"])
        self.assertEqual(row["input_digest"], healed["input_digest"])
        self.assertEqual(row["receipt_id"], healed["receipt_id"])
        # displaced wrong bytes preserved as evidence
        backups = [n for n in os.listdir(config.quarantine_dir())
                   if n.startswith("E1.json.") and ".replaced." in n]
        self.assertEqual(1, len(backups))
        # idempotent: a second sweep changes nothing
        self.assertEqual({"healed_published": 0, "corrected_unfinished": 0},
                         reconcile.reconcile_all(self.conn, "reconcile"))

    def test_healing_converges_to_each_export_own_snapshot(self):
        store.set_rules(self.conn, R1)
        store.submit_export(self.conn, "E1", RECS)
        self.publish("E1")
        store.set_rules(self.conn, R2)
        store.submit_export(self.conn, "E2", RECS)
        self.publish("E2")
        # poison both with the OTHER rules, then heal and verify independence.
        testsupport.poison_published(self.conn, "E1", R2)
        testsupport.poison_published(self.conn, "E2", R1)
        counts = reconcile.reconcile_all(self.conn, "reconcile")
        self.assertEqual(2, counts["healed_published"])
        for eid, redacted_field, kept_field, kept in (
            ("E1", "depth_m", "lat", 31.23),
            ("E2", "lat", "depth_m", 42.5),
        ):
            row = store.get_export(self.conn, eid)
            doc = json.loads(artifacts.load_verified(row))
            self.assertEqual(doc["rules_digest"], row["rules_digest"], eid)
            self.assertEqual("***", doc["records"][0][redacted_field], eid)
            self.assertEqual(kept, doc["records"][0][kept_field], eid)
            self.assertEqual(
                hashlib.sha256(render_artifact_bytes(row)).hexdigest(),
                row["artifact_digest"], eid)


class UnfinishedHealingTest(ReconcileTestBase):
    def test_corrupt_received_row_is_corrected_before_publish(self):
        store.set_rules(self.conn, R1)
        store.submit_export(self.conn, "E1", RECS)
        # simulate stale/derived masking that disagrees with the frozen snapshot
        from app.canonical import canonical
        from app.masking import apply_rules
        bad = canonical(apply_rules(RECS, R2))
        self.conn.execute("UPDATE exports SET masked_records=? WHERE export_id='E1'", (bad,))
        self.conn.commit()
        fencing = store.acquire_lease(
            self.conn, worker.lease_resource("E1"), "w-test", 5)
        result = worker.process_export(self.conn, "E1", "w-test", fencing)
        self.assertEqual("masking_corrected", result)
        # still RECEIVED; a follow-up tick publishes the consistent artifact
        result = worker.process_export(self.conn, "E1", "w-test", fencing)
        self.assertEqual("published", result)
        row = store.get_export(self.conn, "E1")
        doc = json.loads(artifacts.load_verified(row))
        self.assertEqual("***", doc["records"][0]["depth_m"])
        self.assertEqual(31.23, doc["records"][0]["lat"])

    def test_recovery_refuses_to_converge_poisoned_staged_artifact(self):
        store.set_rules(self.conn, R1)
        store.submit_export(self.conn, "E1", RECS)
        from app import recovery
        from app.canonical import canonical
        from app.masking import apply_rules
        bad = canonical(apply_rules(RECS, R2))
        # poisoned STAGED state: stage advanced and wrong masking stored
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E1", "PROCESSING", ("RECEIVED",))
            store.cas_stage(self.conn, "E1", "STAGED", ("PROCESSING",))
            self.conn.execute("UPDATE exports SET masked_records=? WHERE export_id='E1'", (bad,))
        result = recovery.recover_export(self.conn, "E1", "w-test")
        self.assertEqual("masking_corrected", result)
        self.assertEqual("RECEIVED", store.get_export(self.conn, "E1")["stage"])


class BindingVerificationTest(ReconcileTestBase):
    def test_identity_fields_are_all_checked(self):
        store.set_rules(self.conn, R1)
        row = self.publish("E1")
        with open(artifacts.published_path("E1"), "rb") as fh:
            good = fh.read()
        doc = json.loads(good)
        doc["rules_digest"] = "deadbeef"
        self.assertFalse(artifacts.verify_artifact_binding(
            (json.dumps(doc, sort_keys=True, separators=(",", ":")) + "\n").encode(), row))
        self.assertTrue(artifacts.verify_artifact_binding(good, row))


if __name__ == "__main__":
    unittest.main()
