"""Crash recovery + consistency repair.

Runs under the export's lease (worker startup and every tick). For each
unfinished export, consult the journal/artifact records plus on-disk digests:

* the staged temp artifact is complete (digest matches the recorded and the
  deterministically recomputed digest) -> converge: publish that very artifact;
* a published file already exists with the expected digest (crash between the
  atomic link and the DB update) -> converge the bookkeeping;
* anything else (partial write, digest mismatch, missing file, orphans) ->
  clean up the残缺 artifacts and requeue the export.

The consistency scan heals exports whose stored mask/artifact drifted from
their frozen decision (the legacy cross-export mask cache persisted another
export's masking): the frozen mask is realigned with the frozen records+rules
snapshot, and a PUBLISHED export's artifact is atomically republished from the
deterministic render. PUBLISHED is a terminal stage and never regresses —
only the mask/artifact references move.
"""
import hashlib
import os
import uuid

from . import artifacts, config, store
from .render import expected_masked_records, render_artifact_bytes


def _expected(export_row):
    data = render_artifact_bytes(export_row)
    return data, hashlib.sha256(data).hexdigest()


def _converge(conn, export_id, digest, actor, via):
    with store.immediate(conn):
        store.record_artifact(conn, export_id, "published", artifacts.published_path(export_id), digest)
        store.mark_published(conn, export_id, digest, artifacts.published_path(export_id), actor, via)


def recover_export(conn, export_id, actor):
    """Recover one export. Caller must hold the export's lease."""
    export = store.get_export(conn, export_id)
    if not export or export["stage"] == "PUBLISHED":
        return "none"
    _, expected_digest = _expected(export)
    pub = artifacts.published_path(export_id)

    # Case 1: published file already on disk (crash between link and DB update).
    if os.path.exists(pub):
        if artifacts.sha256_file(pub) == expected_digest:
            _converge(conn, export_id, expected_digest, actor, "recovery_published_file")
            artifacts.cleanup_tmp_for(export_id)
            return "converged"
        target = artifacts.quarantine(pub)
        with store.immediate(conn):
            store.journal(conn, export_id, actor, "recovery_quarantined_published", target)

    # Case 2: a staged temp artifact whose digest matches journal + recompute.
    for row in store.staged_artifacts(conn, export_id):
        path = row["path"]
        if (
            os.path.exists(path)
            and artifacts.sha256_file(path) == row["digest"] == expected_digest
        ):
            artifacts.publish(path, pub, row["digest"])
            _converge(conn, export_id, row["digest"], actor, "recovery_staged_artifact")
            artifacts.cleanup_tmp_for(export_id)
            return "converged"

    # Case 3: incomplete/mismatched remains -> clean up and requeue.
    removed = artifacts.cleanup_tmp_for(export_id)
    with store.immediate(conn):
        for row in store.staged_artifacts(conn, export_id):
            store.abort_artifact(conn, row["id"])
        store.journal(conn, export_id, actor, "recovery_cleanup", "removed=%d" % len(removed))
        store.requeue(conn, export_id, actor, "recovery_cleanup removed=%d" % len(removed))
    return "requeued"


def sweep_orphans(conn, actor, older_than_seconds=30.0):
    """Delete temp files not referenced by any staged artifact record."""
    import time

    removed = []
    known = set()
    for export in store.list_exports(conn, limit=1000):
        for row in store.staged_artifacts(conn, export["export_id"]):
            known.add(os.path.abspath(row["path"]))
    now = time.time()
    for path in artifacts.list_tmp_files():
        if os.path.abspath(path) in known:
            continue
        if now - os.path.getmtime(path) < older_than_seconds:
            continue  # may belong to an in-flight staging; leave it alone
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    if removed:
        with store.immediate(conn):
            store.journal(conn, None, actor, "recovery_orphan_sweep", "removed=%d" % len(removed))
    return removed


# ------------------------------------------------------- consistency repair

def _lease_resource(export_id):
    return "export:" + export_id


def _file_digest(path):
    try:
        return artifacts.sha256_file(path)
    except OSError:
        return None


def repair_export(conn, export_id, actor):
    """Converge one export back to its frozen decision. Caller holds the lease.

    Two drift shapes are healed: a frozen mask that disagrees with the frozen
    records+rules snapshot, and a PUBLISHED artifact that disagrees with the
    deterministic render (or is missing/unreadable). PUBLISHED never
    regresses — only the mask/artifact references move.
    """
    export = store.get_export(conn, export_id)
    if not export:
        return "none"
    repaired = False
    expected_mask = expected_masked_records(export)
    if export["masked_records"] != expected_mask:
        with store.immediate(conn):
            repaired = store.correct_masked_records(
                conn, export_id, expected_mask, export["masked_records"], actor)
        export = store.get_export(conn, export_id)
    if export["stage"] != "PUBLISHED":
        # Unfinished exports are taken over by the normal recovery/processing
        # flow, which now renders from the corrected mask.
        return "repaired" if repaired else "consistent"
    data, digest = _expected(export)
    pub = artifacts.published_path(export_id)
    file_ok = (
        export["artifact_digest"] == digest
        and export["artifact_path"] == pub
        and os.path.exists(pub)
        and _file_digest(pub) == digest
    )
    if file_ok:
        return "repaired" if repaired else "consistent"
    # (Re)publish the deterministically rendered artifact. A mismatched file
    # is quarantined for forensics, then the corrected bytes are linked in
    # atomically — the publish primitive still never clobbers.
    tmp = artifacts.tmp_path(export_id, "repair-%s" % uuid.uuid4().hex[:8])
    artifacts.write_tmp(tmp, data)
    if os.path.exists(pub) and _file_digest(pub) != digest:
        target = artifacts.quarantine(pub)
        with store.immediate(conn):
            store.journal(conn, export_id, actor, "repair_quarantined_published", target)
    via = artifacts.publish(tmp, pub, digest)
    with store.immediate(conn):
        store.repair_published_artifact(conn, export_id, digest, pub, actor, "repair_" + via)
    return "repaired"


def scan_consistency(conn, actor, limit=1000):
    """Detect and repair exports whose stored mask/artifact drifted from the
    frozen decision. Each candidate is repaired under its own lease so
    parallel workers cannot interleave repairs. Returns the repaired ids."""
    candidates = []
    for row in store.list_exports(conn, limit=limit):
        try:
            if row["masked_records"] != expected_masked_records(row):
                candidates.append(row["export_id"])
                continue
            if row["stage"] != "PUBLISHED":
                continue
            _, digest = _expected(row)
            if (
                row["artifact_digest"] != digest
                or row["artifact_path"] != artifacts.published_path(row["export_id"])
                or _file_digest(row["artifact_path"]) != digest
            ):
                candidates.append(row["export_id"])
        except Exception:
            continue  # unparseable frozen evidence is not auto-repairable
    repaired = []
    for export_id in candidates:
        fencing = store.acquire_lease(conn, _lease_resource(export_id), actor, config.lease_ttl())
        if fencing is None:
            continue  # another worker is repairing/processing it
        try:
            if repair_export(conn, export_id, actor) == "repaired":
                repaired.append(export_id)
        finally:
            store.release_lease(conn, _lease_resource(export_id), actor, fencing)
    return repaired
