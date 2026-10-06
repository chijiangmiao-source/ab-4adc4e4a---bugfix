"""Test-only support hooks (usable solely when TEST_HOOKS is enabled).

``poison_published`` reproduces the legacy input-only masking-cache defect on a
PUBLISHED export: the downloadable artifact is rewritten so its records follow
a *different* rules document while its frozen identity header (notably
rules_digest) still claims the export's real frozen snapshot. Stage stays
PUBLISHED, exactly mirroring an export affected by the historical bug.
"""
import hashlib
import json
import os

from . import artifacts, store
from .canonical import canonical
from .masking import apply_rules, validate_rules
from .render import render_artifact_bytes


def poison_published(conn, export_id, wrong_rules, actor="test-hook"):
    """Overwrite a PUBLISHED artifact with wrongly-masked bytes.

    Returns the injected digest, or None when the export is missing / not
    PUBLISHED / the supplied rules document is invalid.
    """
    if validate_rules(wrong_rules):
        return None
    row = store.get_export(conn, export_id)
    if not row or row["stage"] != "PUBLISHED":
        return None
    wrong_masked = canonical(apply_rules(json.loads(row["records"]), wrong_rules))
    clone = dict(row)
    clone["masked_records"] = wrong_masked
    data = render_artifact_bytes(clone)  # header keeps the frozen rules_digest
    digest = hashlib.sha256(data).hexdigest()
    dst = artifacts.published_path(export_id)
    tmp = artifacts.tmp_path(export_id, "poison")
    artifacts.write_tmp(tmp, data)
    os.replace(tmp, dst)
    with store.immediate(conn):
        conn.execute(
            "UPDATE exports SET masked_records = ?, artifact_digest = ?, artifact_path = ?, "
            "updated_at = ? WHERE export_id = ?",
            (wrong_masked, digest, dst, store.utcnow(), export_id),
        )
        store.journal(conn, export_id, actor, "test_poisoned_published",
                      "injected_digest=%s" % digest)
    return digest
