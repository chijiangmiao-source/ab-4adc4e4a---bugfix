"""Consistency repair: exports polluted by the legacy cross-export mask cache
must converge to content determined solely by their own frozen records + rules
snapshot — including exports already showing PUBLISHED (stage never regresses).
"""
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest

from app import artifacts, config, recovery, store, worker
from app.canonical import canonical
from app.masking import apply_rules
from app.render import expected_masked_records, render_artifact_bytes

RECORDS = [{"ts": "t0", "lat": 31.2, "depth_m": 10, "vessel_id": "V-1"}]
RULES_B = {"rules": [{"field": "lat", "action": "redact"}]}


class RepairTestBase(unittest.TestCase):
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

    def export(self, export_id="E-1"):
        return store.get_export(self.conn, export_id)

    def submit_and_publish(self, export_id="E-1", records=None):
        store.submit_export(self.conn, export_id, records or RECORDS)
        fencing = store.acquire_lease(self.conn, worker.lease_resource(export_id), "w-test", 5)
        self.assertEqual("published", worker.process_export(self.conn, export_id, "w-test", fencing))
        store.release_lease(self.conn, worker.lease_resource(export_id), "w-test", fencing)
        return self.export(export_id)

    def simulate_legacy_drift(self, export_id, wrong_rules):
        """Reproduce exactly what the legacy input-keyed mask cache persisted:
        frozen mask (and published artifact) computed from another export's
        rules snapshot. Returns the drifted artifact digest."""
        row = self.export(export_id)
        wrong_mask = canonical(apply_rules(json.loads(row["records"]), wrong_rules))
        drifted = dict(row, masked_records=wrong_mask)
        data = render_artifact_bytes(drifted)
        digest = hashlib.sha256(data).hexdigest()
        with store.immediate(self.conn):
            self.conn.execute(
                "UPDATE exports SET masked_records = ?, artifact_digest = ? WHERE export_id = ?",
                (wrong_mask, digest, export_id),
            )
            self.conn.execute(
                "UPDATE artifacts SET digest = ? WHERE export_id = ? AND kind = 'published'",
                (digest, export_id),
            )
        artifacts.write_tmp(artifacts.published_path(export_id), data)
        return digest

    def expected_digest(self, export_id="E-1"):
        return hashlib.sha256(render_artifact_bytes(self.export(export_id))).hexdigest()


class PublishedRepairTest(RepairTestBase):
    def test_drifted_published_export_converges_to_its_frozen_rules(self):
        row = self.submit_and_publish()
        good_digest = row["artifact_digest"]
        published_at = row["published_at"]
        drifted_digest = self.simulate_legacy_drift("E-1", RULES_B)
        self.assertNotEqual(good_digest, drifted_digest)

        repaired = recovery.scan_consistency(self.conn, "w-repair")

        self.assertEqual(["E-1"], repaired)
        row = self.export()
        # mask realigned with the frozen records+rules snapshot
        self.assertEqual(expected_masked_records(row), row["masked_records"])
        # artifact republished from the deterministic render
        self.assertEqual(good_digest, row["artifact_digest"])
        self.assertEqual(good_digest, self.expected_digest())
        data = artifacts.load_verified(row)  # download path serves it
        self.assertEqual(good_digest, hashlib.sha256(data).hexdigest())
        doc = json.loads(data)
        self.assertEqual(31.2, doc["records"][0]["lat"])  # default rules: lat kept
        self.assertNotEqual("V-1", doc["records"][0]["vessel_id"])  # vessel hashed
        # PUBLISHED is terminal: stage and publish timestamp untouched
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(published_at, row["published_at"])
        # exactly one published artifact, registered with the corrected digest
        self.assertEqual(1, len(artifacts.list_published_files()))
        published = store.published_artifacts(self.conn, "E-1")
        self.assertEqual(1, len(published))
        self.assertEqual(good_digest, published[0]["digest"])
        # repair is journaled; the wrong file was quarantined, not clobbered
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("mask_corrected", events)
        self.assertIn("artifact_republished", events)
        self.assertEqual(1, len(os.listdir(config.quarantine_dir())))
        # a second scan finds nothing to do (idempotent)
        self.assertEqual([], recovery.scan_consistency(self.conn, "w-repair"))

    def test_consistent_published_export_is_untouched(self):
        row = self.submit_and_publish()
        digest = row["artifact_digest"]
        self.assertEqual([], recovery.scan_consistency(self.conn, "w-repair"))
        row = self.export()
        self.assertEqual(digest, row["artifact_digest"])
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertNotIn("mask_corrected", events)
        self.assertNotIn("artifact_republished", events)

    def test_missing_published_file_is_restored(self):
        row = self.submit_and_publish()
        digest = row["artifact_digest"]
        os.unlink(artifacts.published_path("E-1"))
        self.assertEqual(["E-1"], recovery.scan_consistency(self.conn, "w-repair"))
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])  # deterministic render
        self.assertEqual(digest, artifacts.sha256_file(artifacts.published_path("E-1")))


class UnfinishedRepairTest(RepairTestBase):
    def test_drifted_received_export_is_processed_with_corrected_mask(self):
        store.submit_export(self.conn, "E-1", RECORDS)
        row = self.export()
        wrong_mask = canonical(apply_rules(json.loads(row["records"]), RULES_B))
        with store.immediate(self.conn):
            self.conn.execute(
                "UPDATE exports SET masked_records = ? WHERE export_id = ?", (wrong_mask, "E-1"))

        self.assertEqual(["E-1"], recovery.scan_consistency(self.conn, "w-repair"))
        row = self.export()
        self.assertEqual("RECEIVED", row["stage"])  # stage untouched
        self.assertEqual(expected_masked_records(row), row["masked_records"])

        fencing = store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-test", 5)
        self.assertEqual("published", worker.process_export(self.conn, "E-1", "w-test", fencing))
        row = self.export()
        self.assertEqual(self.expected_digest(), row["artifact_digest"])
        doc = json.loads(artifacts.load_verified(row))
        self.assertEqual(31.2, doc["records"][0]["lat"])  # default rules: lat kept

    def test_drifted_staged_artifact_is_cleaned_and_reprocessed(self):
        store.submit_export(self.conn, "E-1", RECORDS)
        row = self.export()
        wrong_mask = canonical(apply_rules(json.loads(row["records"]), RULES_B))
        drifted = dict(row, masked_records=wrong_mask)
        data = render_artifact_bytes(drifted)
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "legacy")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            self.conn.execute(
                "UPDATE exports SET masked_records = ? WHERE export_id = ?", (wrong_mask, "E-1"))
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        # one tick: repair the mask, then recovery drops the stale staged
        # artifact and requeues, then the export is processed correctly.
        worker.tick(self.conn, "w-tick")

        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(self.expected_digest(), row["artifact_digest"])
        doc = json.loads(artifacts.load_verified(row))
        self.assertEqual(31.2, doc["records"][0]["lat"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))


class RepairRaceTest(RepairTestBase):
    def test_parallel_workers_repair_exactly_once(self):
        self.submit_and_publish()
        self.simulate_legacy_drift("E-1", RULES_B)
        stop = threading.Event()

        def loop(name):
            conn = store.connect()
            try:
                while not stop.is_set():
                    try:
                        recovery.scan_consistency(conn, name)
                    except Exception:
                        conn.rollback()
                    time.sleep(0.02)
            finally:
                conn.close()

        threads = [threading.Thread(target=loop, args=("w-%d" % i,)) for i in range(2)]
        for t in threads:
            t.start()
        deadline = time.time() + 15
        consistent = False
        while time.time() < deadline:
            row = self.export()
            if (
                row["masked_records"] == expected_masked_records(row)
                and row["artifact_digest"] == self.expected_digest()
                and os.path.exists(artifacts.published_path("E-1"))
                and artifacts.sha256_file(artifacts.published_path("E-1")) == row["artifact_digest"]
            ):
                consistent = True
                break
            time.sleep(0.05)
        time.sleep(0.5)  # give the loser a chance to misbehave
        stop.set()
        for t in threads:
            t.join()

        self.assertTrue(consistent)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))
        self.assertEqual(1, len(artifacts.list_published_files()))
        self.assertEqual(self.expected_digest(), row["artifact_digest"])
        self.assertEqual([], recovery.scan_consistency(self.conn, "w-final"))


if __name__ == "__main__":
    unittest.main()
