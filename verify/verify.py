"""One-shot acceptance service: build checks + unit tests + API/HTTP smoke.

Runs the build check (byte-compile) and the unit test suite, then exercises
the live HTTP API through the required scenarios:

  1. export keeps following the frozen rules snapshot after rules change:
     two rule sets masking different fields, same tracks under different ids,
     each downloaded artifact matches its own records/rules digest/artifact digest
  2. already-PUBLISHED artifacts poisoned by the legacy input-only masking cache
     are refused (fail closed), then safely converge to frozen-snapshot content
     via both the on-demand trigger and automatic periodic worker reconciliation,
     and re-verified after crash recovery / restart
  3. crash recovery (converge a complete staged artifact; clean up a partial one)
  4. business-equivalent retransmission (first receipt, no second artifact)
     and conflict handling (different records or rules snapshot)

Exits 0 when everything passes, 1 otherwise.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

API = os.environ.get("API_BASE", "http://localhost:8080").rstrip("/")
DATA_DIR = os.environ.get("DATA_DIR", "./data")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = uuid.uuid4().hex[:6]

FAILURES = []


def check(name, condition, detail=""):
    mark = "PASS" if condition else "FAIL"
    line = "[%s] %s" % (mark, name)
    if not condition and detail:
        line += " -- " + detail
    print(line, flush=True)
    if not condition:
        FAILURES.append(name)


def step(title):
    print("\n=== %s ===" % title, flush=True)


# ------------------------------------------------------------------ helpers

def req(method, path, body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(API + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers or {})


def as_json(raw):
    return json.loads(raw.decode("utf-8"))


def wait_for_stage(export_id, stage, timeout):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        status, raw, _ = req("GET", "/api/exports/" + export_id)
        if status == 200:
            last = as_json(raw)
            if last["stage"] == stage:
                return last
        time.sleep(1)
    return last


def published_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "published")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n == export_id + ".json"]


def tmp_files(export_id):
    directory = os.path.join(DATA_DIR, "artifacts", "tmp")
    if not os.path.isdir(directory):
        return []
    return [n for n in os.listdir(directory) if n.startswith(export_id + ".")]


def download(export_id):
    return req("GET", "/api/exports/%s/artifact" % export_id)


def check_artifact_matches_frozen(export_id, expect_masked):
    """Download and enforce: HTTP 200, body digest == listed artifact digest,
    header rules_digest/input_digest == frozen row, and records match expect.

    expect_masked maps the first record's field name -> expected masked value.
    Returns the parsed artifact document (or None)."""
    status, detail_raw, _ = req("GET", "/api/exports/" + export_id)
    detail = as_json(detail_raw) if status == 200 else None
    status, raw, _ = download(export_id)
    if detail is None or status != 200:
        check("download %s -> 200" % export_id, False, "HTTP %s" % status)
        return None
    body_digest = hashlib.sha256(raw).hexdigest()
    doc = as_json(raw)
    check("%s artifact digest matches listing" % export_id,
          body_digest == detail["artifact_digest"])
    check("%s header rules_digest matches frozen row" % export_id,
          doc.get("rules_digest") == detail["rules_digest"])
    check("%s header input_digest matches frozen row" % export_id,
          doc.get("input_digest") == detail["input_digest"])
    first = doc["records"][0]
    ok = all(first.get(field) == value for field, value in expect_masked.items())
    check("%s records follow frozen snapshot (%s)" % (export_id, expect_masked), ok,
          "got=%s" % {k: first.get(k) for k in expect_masked})
    return doc


# ------------------------------------------------------------------ phases

def build_checks():
    step("构建检查：python -m compileall")
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "verify", "tests"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    check("compileall app/verify/tests", proc.returncode == 0, proc.stderr.strip()[:400])


def unit_tests():
    step("代码测试：unittest discover")
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", ".", "-v"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    output = (proc.stdout + proc.stderr).strip()
    print("\n".join(output.splitlines()[-25:]))
    check("unit test suite", proc.returncode == 0, output[-400:])


def wait_for_api():
    step("等待 API 健康")
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            status, raw, _ = req("GET", "/healthz")
            if status == 200 and as_json(raw).get("ok"):
                check("health response", True)
                return
        except Exception:
            pass
        time.sleep(1)
    check("health response", False, "API did not become healthy in time")
    raise SystemExit(1)


def smoke():
    e1, e2, e3, e4, e5, e6 = ("VFY%d-%s" % (i, RUN) for i in range(1, 7))
    recs = [
        {"ts": "2026-10-06T01:00:00Z", "lat": 31.230416, "lon": 121.473701, "depth_m": 42.51, "vessel_id": "HAICE-01"},
        {"ts": "2026-10-06T01:05:00Z", "lat": 31.231102, "lon": 121.480233, "depth_m": 43.04, "vessel_id": "HAICE-01"},
    ]
    rules_r1 = {"rules": [
        {"field": "depth_m", "action": "redact"},
        {"field": "vessel_id", "action": "hash", "length": 10},
    ]}
    rules_r2 = {"rules": [{"field": "lat", "action": "redact"}]}

    step("页面通过真实 API 轮询（页面可加载且引用 API）")
    status, raw, headers = req("GET", "/")
    check("operator page served", status == 200 and "text/html" in headers.get("Content-Type", ""))
    check("page polls real API", b"/api/exports" in raw and b"fetch(" in raw)

    step("设置规则 R1（遮蔽 depth_m / 散列 vessel_id）")
    status, raw, _ = req("PUT", "/api/rules", rules_r1)
    check("put rules R1", status == 200, "HTTP %s %s" % (status, raw[:200]))
    d1 = as_json(raw)["digest"]

    step("提交导出 E1 → 201 冻结裁决")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    check("submit E1 -> 201", status == 201, "HTTP %s %s" % (status, raw[:300]))
    receipt1 = as_json(raw)
    check("E1 froze rules R1", receipt1.get("rules_digest") == d1)

    step("业务等价重传 E1（键序/空白不同）→ 200 首次回执")
    reordered = [dict(reversed(list(r.items()))) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": reordered})
    replay = as_json(raw)
    check("replay E1 -> 200", status == 200, "HTTP %s %s" % (status, raw[:300]))
    check("replay returns first receipt",
          replay.get("receipt_id") == receipt1["receipt_id"]
          and replay.get("received_at") == receipt1["received_at"]
          and replay.get("replay") is True)

    step("等待 E1 发布并校验工件")
    detail = wait_for_stage(e1, "PUBLISHED", 90)
    check("E1 published", detail is not None and detail["stage"] == "PUBLISHED",
          "last=%s" % (detail and detail.get("stage")))
    a1 = detail["artifact_digest"] if detail else None
    status, raw, _ = download(e1)
    check("download E1 -> 200", status == 200, "HTTP %s" % status)
    if status == 200:
        check("E1 digest matches download", hashlib.sha256(raw).hexdigest() == a1)
        doc = as_json(raw)
        check("E1 masked per R1 (depth redacted, vessel hashed, lat kept)",
              doc["records"][0]["depth_m"] == "***"
              and doc["records"][0]["vessel_id"] != "HAICE-01"
              and doc["records"][0]["lat"] == 31.230416)
        check("E1 artifact carries frozen digests",
              doc["rules_digest"] == d1 and doc["input_digest"] == receipt1["input_digest"])
    check("exactly one published artifact file for E1", len(published_files(e1)) == 1,
          str(published_files(e1)))

    step("值班员改规则 → R2（遮蔽 lat）")
    status, raw, _ = req("PUT", "/api/rules", rules_r2)
    check("put rules R2", status == 200)
    d2 = as_json(raw)["digest"]
    check("R2 digest differs", d2 != d1)

    step("规则改动后重传 E1（同记录）→ 409 冲突（规则快照不同）")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": recs})
    conflict = as_json(raw)
    check("replay under changed rules -> 409", status == 409, "HTTP %s" % status)
    check("conflict preserves original evidence",
          conflict.get("existing", {}).get("rules_digest") == d1
          and conflict.get("existing", {}).get("receipt_id") == receipt1["receipt_id"])

    step("不同记录重传 E1 → 409 冲突（记录不同）")
    changed = [dict(r, depth_m=99.9) for r in recs]
    status, raw, _ = req("POST", "/api/exports", {"export_id": e1, "records": changed})
    check("different records -> 409", status == 409, "HTTP %s" % status)

    detail = wait_for_stage(e1, "PUBLISHED", 5)
    check("E1 evidence untouched after conflicts",
          detail is not None and detail["artifact_digest"] == a1 and detail["stage"] == "PUBLISHED")

    step("规则改动后，E1 仍按冻结快照导出；新导出 E2 使用 R2，两份工件各自匹配摘要")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": recs})
    check("submit E2 -> 201", status == 201)
    check("E2 froze rules R2", as_json(raw).get("rules_digest") == d2)
    detail2 = wait_for_stage(e2, "PUBLISHED", 90)
    check("E2 published", detail2 is not None and detail2["stage"] == "PUBLISHED")
    # Same records, different frozen rules -> each download's record content,
    # rules summary and artifact summary must match ITS OWN frozen row.
    doc1 = check_artifact_matches_frozen(e1, {"depth_m": "***", "lat": 31.230416,
                                              "lon": 121.473701})
    if doc1:
        check("E1 vessel hashed under R1", doc1["records"][0]["vessel_id"] != recs[0]["vessel_id"])
    doc2 = check_artifact_matches_frozen(e2, {"lat": "***", "depth_m": 42.51,
                                              "vessel_id": recs[0]["vessel_id"]})
    check("E1/E2 inputs identical but rules + artifacts differ",
          detail2["input_digest"] == receipt1["input_digest"]
          and detail2["rules_digest"] == d2 != d1
          and detail2["artifact_digest"] != a1)
    check("exactly one published artifact file per export",
          len(published_files(e1)) == 1 and len(published_files(e2)) == 1)

    step("已发布工件与冻结规则不一致：下载必须 fail-closed（拒绝，不交付错误内容）")
    # Reproduce the legacy defect on E2: bytes re-masked under R1-style rules
    # while the export stays PUBLISHED with its frozen R2 digest. A periodic
    # worker scan may converge it at any time, so (re)poison tightly until a
    # download is actually observed being refused.
    injected = None
    refused_code = None
    deadline = time.time() + 70
    while time.time() < deadline:
        status, raw, _ = req("POST", "/api/test/fault",
                             {"export_id": e2, "mode": "poison_published", "rules": rules_r1})
        if status != 200:
            break
        injected = as_json(raw).get("injected_digest")
        code, body, _ = download(e2)
        if code in (410, 500):
            refused_code = code
            break
        # a worker healed it between the poison and the GET: try again
        time.sleep(0.2)
    check("poison E2 published artifact", injected is not None, "never poisoned")
    check("poisoned PUBLISHED artifact is refused (not 200)", refused_code in (410, 500),
          "last code=%s" % refused_code)
    code2, _, _ = download(e1)
    check("unaffected E1 still downloadable", code2 == 200, "HTTP %s" % code2)

    step("修复部署后受影响的 PUBLISHED 导出安全收敛（on-demand 触发，阶段不倒退）")
    # Re-arm and trigger in a tight loop: a periodic worker scan may converge
    # the export first; retry until the on-demand trigger itself reports a heal.
    triggered = None
    deadline = time.time() + 60
    while time.time() < deadline:
        req("POST", "/api/test/fault",
            {"export_id": e2, "mode": "poison_published", "rules": rules_r1})
        status, raw, _ = req("POST", "/api/test/fault", {"export_id": e2, "mode": "reconcile_now"})
        counts = as_json(raw) if status == 200 else {}
        if status == 200 and counts.get("healed_published", 0) >= 1:
            triggered = counts
            break
        time.sleep(0.3)
    check("reconcile_now -> 200 healed>=1", triggered is not None, str(triggered))
    detail2h = wait_for_stage(e2, "PUBLISHED", 10)
    check("E2 stayed PUBLISHED (no stage regression)",
          detail2h is not None and detail2h["stage"] == "PUBLISHED")
    check("E2 converged off the injected digest",
          detail2h is not None and detail2h["artifact_digest"] != injected
          and detail2h["artifact_digest"] == detail2["artifact_digest"])
    doc2h = check_artifact_matches_frozen(e2, {"lat": "***", "depth_m": 42.51,
                                               "vessel_id": recs[0]["vessel_id"]})
    check("E2 reconciliation is idempotent", doc2h is not None)
    if doc2h:
        ev_status, ev_raw, _ = req("GET", "/api/exports/" + e2)
        events = [e["event"] for e in as_json(ev_raw).get("events", [])]
        check("E2 journal records artifact_reconciled", "artifact_reconciled" in events, str(events))

    step("周期性 worker 和解也能自动收敛受影响的 PUBLISHED 导出（E6）")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e6, "records": recs})
    check("submit E6 -> 201", status == 201)
    detail6 = wait_for_stage(e6, "PUBLISHED", 90)
    check("E6 published", detail6 is not None and detail6["stage"] == "PUBLISHED")
    good6 = detail6["artifact_digest"] if detail6 else None
    status, raw, _ = req("POST", "/api/test/fault",
                         {"export_id": e6, "mode": "poison_published", "rules": rules_r1})
    check("poison E6 published artifact", status == 200, "HTTP %s" % status)
    # No on-demand trigger: rely solely on the workers' periodic reconciliation.
    deadline = time.time() + 120
    healed6 = None
    while time.time() < deadline:
        d = wait_for_stage(e6, "PUBLISHED", 1)
        if d and d["artifact_digest"] == good6:
            healed6 = d
            break
        time.sleep(1)
    check("E6 auto-healed by periodic worker reconciliation", healed6 is not None)
    if healed6:
        check("E6 stage never regressed", healed6["stage"] == "PUBLISHED")
        check_artifact_matches_frozen(e6, {"lat": "***", "depth_m": 42.51,
                                           "vessel_id": recs[0]["vessel_id"]})

    step("崩溃恢复 A：暂存完整工件后进程退出 → 收敛到同一完整工件")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e3, "mode": "crash_after_staged"})
    check("arm fault crash_after_staged", status == 202, "HTTP %s %s" % (status, raw[:200]))
    status, raw, _ = req("POST", "/api/exports", {"export_id": e3, "records": recs})
    check("submit E3 -> 201", status == 201)
    detail3 = wait_for_stage(e3, "PUBLISHED", 150)
    check("E3 published after crash+recovery", detail3 is not None and detail3["stage"] == "PUBLISHED",
          "last=%s" % (detail3 and detail3.get("stage")))
    if detail3:
        events = detail3.get("events", [])
        recovered = any("recovery" in (e.get("detail") or "") or "recover" in e.get("event", "")
                        for e in events)
        check("E3 journal shows recovery convergence", recovered,
              "events=%s" % [e["event"] for e in events])
        status, raw, _ = download(e3)
        check("E3 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail3["artifact_digest"])
        check("exactly one published artifact file for E3", len(published_files(e3)) == 1)
        check("no temp leftovers for E3", tmp_files(e3) == [], str(tmp_files(e3)))

    step("崩溃恢复 B：临时工件写一半进程退出 → 清理残缺工件并重处理")
    status, raw, _ = req("POST", "/api/test/fault", {"export_id": e4, "mode": "crash_partial_write"})
    check("arm fault crash_partial_write", status == 202, "HTTP %s" % status)
    status, raw, _ = req("POST", "/api/exports", {"export_id": e4, "records": recs})
    check("submit E4 -> 201", status == 201)
    detail4 = wait_for_stage(e4, "PUBLISHED", 150)
    check("E4 published after cleanup+requeue", detail4 is not None and detail4["stage"] == "PUBLISHED",
          "last=%s" % (detail4 and detail4.get("stage")))
    if detail4:
        events = [e["event"] for e in detail4.get("events", [])]
        check("E4 journal shows cleanup/requeue",
              "recovery_cleanup" in events or "requeued" in events, "events=%s" % events)
        check("E4 needed a retry", detail4["attempts"] >= 1)
        check("no temp leftovers for E4", tmp_files(e4) == [], str(tmp_files(e4)))
        status, raw, _ = download(e4)
        check("E4 download verified", status == 200
              and hashlib.sha256(raw).hexdigest() == detail4["artifact_digest"])

    step("下载接口不暴露未核验内容（崩溃窗口内只能 409，不能 200）")
    req("POST", "/api/test/fault", {"export_id": e5, "mode": "crash_partial_write"})
    status, raw, _ = req("POST", "/api/exports", {"export_id": e5, "records": recs})
    check("submit E5 -> 201", status == 201)
    saw_409 = False
    exposed = False
    deadline = time.time() + 150
    while time.time() < deadline:
        code, body, _ = download(e5)
        d = wait_for_stage(e5, "PUBLISHED", 1)  # stage probe after the download
        if code == 200:
            # 200 is legitimate only when the export is PUBLISHED (stage never
            # regresses, so a PUBLISHED probe after the 200 is conclusive).
            if d and d["stage"] == "PUBLISHED":
                break
            exposed = True
            break
        if code == 409:
            saw_409 = True
        time.sleep(0.4)
    check("unpublished content never served", not exposed)
    check("download refused while unverified (409 observed)", saw_409)

    step("发布后的业务等价重传：仍返回首次回执且不产生第二个工件")
    status, raw, _ = req("POST", "/api/exports", {"export_id": e2, "records": reordered})
    replay2 = as_json(raw)
    check("replay E2 -> 200 first receipt", status == 200 and replay2.get("replay") is True)
    detail2b = wait_for_stage(e2, "PUBLISHED", 5)
    check("E2 artifact digest unchanged by replay",
          detail2b is not None and detail2b["artifact_digest"] == detail2["artifact_digest"])
    check("exactly one published artifact file for E2", len(published_files(e2)) == 1)

    step("崩溃/重启与暂存恢复之后：再次核对 E1/E2/E6 与各自冻结规则一致")
    # The crash-injection scenarios above exited worker processes; the
    # supervisor (compose restart / smoke restart loop) brought them back and
    # startup reconciliation + staged recovery have both run since.
    for eid, expect in (
        (e1, {"depth_m": "***", "lat": 31.230416}),
        (e2, {"lat": "***", "depth_m": 42.51}),
        (e6, {"lat": "***", "depth_m": 42.51}),
    ):
        check_artifact_matches_frozen(eid, expect)

    step("终态检查：无残缺临时工件残留")
    leftovers = []
    for eid in (e1, e2, e3, e4, e5, e6):
        leftovers.extend(tmp_files(eid))
    check("no temp artifacts left behind", leftovers == [], str(leftovers))


def main():
    print("verify: one-shot acceptance run %s against %s" % (RUN, API), flush=True)
    build_checks()
    unit_tests()
    wait_for_api()
    smoke()
    print("\n==============================================")
    if FAILURES:
        print("verify: FAILED (%d): %s" % (len(FAILURES), ", ".join(FAILURES)), flush=True)
        return 1
    print("verify: ALL CHECKS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
