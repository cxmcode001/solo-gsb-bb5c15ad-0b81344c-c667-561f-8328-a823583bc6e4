"""End-to-end demo using only the Python standard library.

Submits three batches of traces against a running server:
  * normal fast traces   -> budgeted stable-hash sampling
  * ERROR traces         -> always kept
  * slow traces          -> always kept

Then polls the decision API until everything is decided and prints a
summary plus the per-minute budget usage.

Usage:
    python demo/demo.py [base_url] [normal_count] [budget]

Example:
    python demo/demo.py http://localhost:8080 300 100
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request


def call(method: str, url: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def make_spans(normal_count: int) -> list[dict]:
    now = time.time_ns()
    spans: list[dict] = []

    for i in range(normal_count):
        tid = f"normal-{i:04d}"
        spans.append(
            {
                "trace_id": tid,
                "span_id": f"{tid}-root",
                "start_time": now + i * 1_000_000,
                "end_time": now + i * 1_000_000 + 50_000_000,  # 50 ms
                "status": "OK",
                "service_name": "shop-api",
                "attributes": {"http.route": "/products", "demo.index": i},
            }
        )

    for i in range(3):
        tid = f"error-{i:02d}"
        spans.append(
            {
                "trace_id": tid,
                "span_id": f"{tid}-root",
                "start_time": now + i * 1_000_000,
                "end_time": now + i * 1_000_000 + 120_000_000,
                "status": "ERROR",
                "service_name": "payments",
                "attributes": {"error": True, "http.status_code": 500},
            }
        )

    for i in range(2):
        tid = f"slow-{i:02d}"
        spans.append(
            {
                "trace_id": tid,
                "span_id": f"{tid}-root",
                "start_time": now + i * 1_000_000,
                "end_time": now + i * 1_000_000 + 2_500_000_000,  # 2.5 s
                "status": "OK",
                "service_name": "search",
                "attributes": {"slow": True},
            }
        )

    return spans


def main() -> int:
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    normal_count = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    budget = int(sys.argv[3]) if len(sys.argv) > 3 else 100

    trace_ids = (
        [f"normal-{i:04d}" for i in range(normal_count)]
        + [f"error-{i:02d}" for i in range(3)]
        + [f"slow-{i:02d}" for i in range(2)]
    )

    print(f"POST /api/v1/ingest  ({len(trace_ids)} traces) -> {base_url}")
    result = call(base_url + "/api/v1/ingest", {"spans": make_spans(normal_count)})
    print("  accepted:", result["new_spans"], "new spans,",
          result["duplicate_spans"], "duplicates")

    print("Waiting for quiet window + minute finalization ...")
    deadline = time.time() + 150
    decisions: dict[str, dict] = {}
    while time.time() < deadline:
        resp = call(
            base_url + "/api/v1/decisions",
            {"trace_ids": trace_ids[:900]},
        )
        decisions = {d["trace_id"]: d for d in resp["decisions"]}
        pending = [
            tid for tid, d in decisions.items() if d["status"] in ("open", "pending")
        ]
        if not pending:
            break
        print(f"  {len(pending)} traces still open/pending ...")
        time.sleep(5)

    by_reason: dict[str, int] = {}
    kept = dropped = 0
    for tid, d in decisions.items():
        by_reason[f"{d['decision']}:{d['reason']}"] = (
            by_reason.get(f"{d['decision']}:{d['reason']}", 0) + 1
        )
        kept += d["decision"] == "kept"
        dropped += d["decision"] == "dropped"

    print("\nDecision summary:")
    for key in sorted(by_reason):
        print(f"  {key:32s} {by_reason[key]}")
    print(f"  {'TOTAL kept':32s} {kept}")
    print(f"  {'TOTAL dropped':32s} {dropped}")
    print(f"  (budget for normal traces = {budget}/minute)")

    budgets = call(base_url + "/api/v1/budgets?limit=3")
    print("\nRecent budget minutes:")
    for b in budgets["budgets"]:
        print(
            f"  {b['minute']}  error={b['error_kept']} slow={b['slow_kept']} "
            f"hashed={b['normal_kept']} finalized={b['finalized']}"
        )

    sample = next(
        (d for d in decisions.values() if d["reason"] == "hashed"), None
    )
    if sample:
        print("\nExample kept-by-hash decision:")
        print(json.dumps(sample, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.URLError as exc:
        print(f"cannot reach server: {exc}", file=sys.stderr)
        raise SystemExit(1)
