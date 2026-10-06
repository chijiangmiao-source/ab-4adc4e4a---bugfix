"""Self-healing of frozen exports whose artifact (or stored masking) disagrees
with their frozen records + rules snapshot.

Legacy versions derived a new export's masked records from a cache keyed only
by ``input_digest``. Identical records submitted under a changed rules snapshot
could be published with the *older* rules' masking while the row (and the
artifact header) claimed the *new* rules' digest: the downloadable artifact
disagrees with its frozen rules summary, and crash recovery recomputes from the
same poisoned stored masking, so the wrong content survives restarts.

Reconciliation runs at startup (app + each worker) and periodically:

* **PUBLISHED + inconsistent** -> regenerate from the frozen records + frozen
  rules snapshot, hard-link the displaced (wrong) bytes into quarantine, then
  atomically swap the file and repoint the row inside one transaction. The
  stage never leaves PUBLISHED (no regression, no second publish cycle).
* **unfinished + inconsistent stored masking** -> correct the derived masking,
  abort stale staged artifacts, quarantine foreign published bytes and requeue
  (RECEIVED stays RECEIVED), so the normal single-publish pipeline regenerates.

Frozen evidence (records, rules snapshot, input/rules digests, receipt) is
never touched; stage terminality is preserved.
"""
import hashlib
import os
import uuid

from . import artifacts, config, store
from .render import render_artifact_bytes


def expected_digest_for_row(export_row):
    """Artifact digest the export SHOULD have per its frozen records + rules."""
    fixed = dict(export_row)
    fixed["masked_records"] = artifacts.expected_masked_records(export_row)
    return hashlib.sha256(render_artifact_bytes(fixed)).hexdigest()


def _unlink_quiet(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _quarantine_link(src):
    """Hard-link displaced bytes into quarantine (evidence preserved)."""
    os.makedirs(config.quarantine_dir(), exist_ok=True)
    backup = os.path.join(
        config.quarantine_dir(),
        "%s.replaced.%s" % (os.path.basename(src), uuid.uuid4().hex[:8]),
    )
    os.link(src, backup)
    return backup


def heal_published(conn, export_row, actor):
    """Heal one PUBLISHED export in place. Returns True when a swap happened."""
    export_id = export_row["export_id"]
    recorded_digest = export_row.get("artifact_digest")
    expected_digest = expected_digest_for_row(export_row)
    dst = artifacts.published_path(export_id)

    # Healthy: recorded digest follows the frozen snapshot AND the on-disk bytes
    # match that digest. Any divergence (wrong digest, tampered/missing file)
    # triggers regeneration.
    current_path = export_row.get("artifact_path") or dst
    if (
        recorded_digest == expected_digest
        and os.path.exists(current_path)
        and artifacts.sha256_file(current_path) == expected_digest
    ):
        return False

    fixed = dict(export_row)
    fixed["masked_records"] = artifacts.expected_masked_records(export_row)
    data = render_artifact_bytes(fixed)
    tmp = artifacts.tmp_path(export_id, "heal-" + uuid.uuid4().hex[:8])
    artifacts.write_tmp(tmp, data)

    with store.immediate(conn):
        # Re-read under the write lock: a concurrent healer may have converged.
        cur = conn.execute(
            "SELECT stage, artifact_digest, artifact_path FROM exports WHERE export_id = ?",
            (export_id,),
        ).fetchone()
        if cur is None or cur["stage"] != "PUBLISHED":
            _unlink_quiet(tmp)
            return False
        live_path = cur["artifact_path"] or dst
        if (
            cur["artifact_digest"] == expected_digest
            and os.path.exists(live_path)
            and artifacts.sha256_file(live_path) == expected_digest
        ):
            _unlink_quiet(tmp)
            return False
        backup = _quarantine_link(live_path) if os.path.exists(live_path) else None
        os.replace(tmp, dst)  # atomic same-FS swap; no missing-file window
        conn.execute(
            "UPDATE exports SET masked_records = ?, artifact_digest = ?, artifact_path = ?, "
            "updated_at = ? WHERE export_id = ?",
            (fixed["masked_records"], expected_digest, dst, store.utcnow(), export_id),
        )
        store.journal(
            conn, export_id, actor, "artifact_reconciled",
            "old_digest=%s new_digest=%s backup=%s" % (recorded_digest, expected_digest, backup),
        )
    return True


def heal_unfinished(conn, export_row, actor):
    """Correct stale derived masking on an unfinished export and requeue it."""
    # Re-read fresh: a concurrent healer may have just converged this row.
    fresh = store.get_export(conn, export_row["export_id"])
    if fresh is None or fresh["stage"] == "PUBLISHED":
        return False
    export_row = fresh
    export_id = export_row["export_id"]
    expected_masked = artifacts.expected_masked_records(export_row)
    if export_row.get("masked_records") == expected_masked:
        return False

    expected_digest = expected_digest_for_row(export_row)
    dst = artifacts.published_path(export_id)
    quarantined = None
    # A wrong-content file may occupy the published path (legacy crash window);
    # quarantine it so the later republish never trips over a foreign digest.
    if os.path.exists(dst) and artifacts.sha256_file(dst) != expected_digest:
        quarantined = artifacts.quarantine(dst)
    artifacts.cleanup_tmp_for(export_id)
    with store.immediate(conn):
        for row in store.staged_artifacts(conn, export_id):
            store.abort_artifact(conn, row["id"])
    store.correct_frozen_masking(
        conn, export_id, expected_masked, actor,
        "stored masking disagreed with frozen rules snapshot"
        + ("; quarantined=%s" % quarantined if quarantined else ""),
    )
    return True


def reconcile_all(conn, actor="reconcile"):
    """Converge every inconsistent export; one bad row never aborts the sweep."""
    counts = {"healed_published": 0, "corrected_unfinished": 0}
    for row in store.all_exports(conn):
        try:
            if row["stage"] == "PUBLISHED":
                if heal_published(conn, row, actor):
                    counts["healed_published"] += 1
            elif heal_unfinished(conn, row, actor):
                counts["corrected_unfinished"] += 1
        except Exception as exc:  # noqa: BLE001 - keep healing the other rows
            with store.immediate(conn):
                store.journal(conn, row["export_id"], actor, "reconcile_failed", repr(exc)[:300])
    return counts
