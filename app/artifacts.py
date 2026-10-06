"""Artifact filesystem operations: temp write, digest verify, atomic publish.

Publish uses hard-link + unlink on the same filesystem: atomic, and it never
clobbers an already-published artifact (second publisher gets FileExistsError
and must verify the existing digest instead).

`load_verified` is fail-closed: besides the on-disk digest check, the artifact
content must follow the export's own frozen records + rules snapshot. A
PUBLISHED artifact that fails that binding (e.g. produced by the legacy
input-only masking cache) is never served.
"""
import hashlib
import json
import os
import time

from . import config
from .canonical import canonical
from .masking import apply_rules


class PublishedMismatch(Exception):
    """A different artifact already occupies the published path."""


class ArtifactMissing(Exception):
    pass


class DigestMismatch(Exception):
    pass


class FrozenSnapshotMismatch(Exception):
    """Artifact bytes do not follow the export's frozen rules snapshot."""


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def tmp_path(export_id, token):
    return os.path.join(config.tmp_dir(), "%s.%s.part" % (export_id, token))


def published_path(export_id):
    return os.path.join(config.published_dir(), "%s.json" % export_id)


def write_tmp(path, data):
    with open(path, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish(tmp, dst, expected_digest):
    """Atomically publish tmp at dst without clobbering. Returns 'linked' or 'dedup'."""
    if os.path.exists(dst):
        if sha256_file(dst) == expected_digest:
            if os.path.exists(tmp):
                os.unlink(tmp)
            return "dedup"
        raise PublishedMismatch("published path holds different content: %s" % dst)
    try:
        os.link(tmp, dst)
    except FileExistsError:
        if sha256_file(dst) == expected_digest:
            os.unlink(tmp)
            return "dedup"
        raise PublishedMismatch("published path holds different content: %s" % dst)
    os.unlink(tmp)
    _fsync_dir(os.path.dirname(dst))
    return "linked"


def quarantine(path):
    os.makedirs(config.quarantine_dir(), exist_ok=True)
    target = os.path.join(
        config.quarantine_dir(),
        "%s.%d" % (os.path.basename(path), int(time.time() * 1000)),
    )
    os.replace(path, target)
    return target


def expected_masked_records(export_row):
    """Deterministically re-mask the frozen records with the frozen snapshot."""
    return canonical(apply_rules(
        json.loads(export_row["records"]),
        json.loads(export_row["rules_snapshot"]),
    ))


def verify_artifact_binding(data, export_row):
    """True iff the artifact bytes both match the recorded digest AND follow the
    export's own frozen records + rules snapshot (its claimed identity)."""
    if sha256_bytes(data) != export_row.get("artifact_digest"):
        return False
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(doc, dict):
        return False
    identity_ok = (
        doc.get("export_id") == export_row["export_id"]
        and doc.get("input_digest") == export_row["input_digest"]
        and doc.get("rules_digest") == export_row["rules_digest"]
        and doc.get("rules_version") == export_row["rules_version"]
        and doc.get("received_at") == export_row["received_at"]
    )
    if not identity_ok:
        return False
    records = doc.get("records")
    if not isinstance(records, list):
        return False
    return canonical(records) == expected_masked_records(export_row)


def tmp_files_for(export_id):
    prefix = export_id + "."
    out = []
    directory = config.tmp_dir()
    if os.path.isdir(directory):
        for name in os.listdir(directory):
            if name.startswith(prefix) and name.endswith(".part"):
                out.append(os.path.join(directory, name))
    return out


def cleanup_tmp_for(export_id):
    removed = []
    for path in tmp_files_for(export_id):
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    return removed


def list_tmp_files():
    directory = config.tmp_dir()
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory) if name.endswith(".part")]


def list_published_files():
    directory = config.published_dir()
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory) if name.endswith(".json")]


def load_verified(export_row):
    """Read a published artifact only when it matches the frozen decision.

    Fail-closed: it must (a) exist, (b) match the recorded artifact digest, and
    (c) follow the frozen records + rules snapshot it claims. The download path
    must never expose content whose masking disagrees with its own frozen rules
    summary (or any other frozen-identity field).
    """
    path = export_row.get("artifact_path")
    if not path or not os.path.exists(path):
        raise ArtifactMissing("artifact file is missing")
    with open(path, "rb") as fh:
        data = fh.read()
    if sha256_bytes(data) != export_row.get("artifact_digest"):
        raise DigestMismatch("artifact digest mismatch")
    if not verify_artifact_binding(data, export_row):
        raise FrozenSnapshotMismatch("artifact does not follow frozen snapshot")
    return data
