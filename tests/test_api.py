"""HTTP-level tests using FastAPI's TestClient."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path):
    # conftest.py already exports test-friendly env vars before the app
    # modules were imported, so quiet window/budget are set. Each test gets
    # an isolated SQLite file by mutating the shared frozen settings object.
    from app import config, main

    object.__setattr__(config.settings, "db_path", str(tmp_path / "api.db"))
    with TestClient(main.app) as c:
        yield c


def _span(tid, status="OK", duration_ms=50):
    start = 1_700_000_000_000_000_000  # explicit epoch nanoseconds
    return {
        "trace_id": tid,
        "span_id": f"{tid}-root",
        "start_time": start,
        "end_time": start + int(duration_ms * 1_000_000),
        "status": status,
        "service_name": "svc-a",
        "attributes": {"k": "v"},
    }


def test_health_and_root(client):
    assert client.get("/health").json() == {"status": "ok"}
    info = client.get("/").json()
    assert info["service"] == "trace-tail-sampler"
    assert info["sample_budget_per_minute"] == 5


def test_ingest_and_query_decisions(client):
    resp = client.post(
        "/api/v1/ingest", json={"spans": [_span("t1"), _span("t2", status="ERROR")]}
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["new_spans"] == 2 and body["duplicate_spans"] == 0

    # Re-submitting the same spans is idempotent.
    resp = client.post(
        "/api/v1/ingest", json={"spans": [_span("t1"), _span("t2", status="ERROR")]}
    )
    assert resp.json()["new_spans"] == 0

    resp = client.post("/api/v1/decisions", json={"trace_ids": ["t1", "t2", "nope"]})
    by_id = {d["trace_id"]: d for d in resp.json()["decisions"]}
    assert by_id["t1"]["status"] == "open"
    assert by_id["nope"]["decision"] == "unknown"
    assert by_id["nope"]["reason"] == "trace_not_found"


def test_end_to_end_decisions(client):
    """Full HTTP flow against the real wall clock.

    With quiet=0.05s the error trace is decided immediately; ordinary
    traces are finalized once their UTC minute ends + quiet window, which
    takes up to ~60s.
    """
    import time

    ids = [f"ok-{i:03d}" for i in range(12)] + ["err-0"]
    spans = [_span(t) for t in ids if t.startswith("ok")]
    spans += [_span("err-0", status="ERROR")]
    client.post("/api/v1/ingest", json={"spans": spans})

    # Wait until all traces reach a terminal state.
    decisions = {}
    deadline = time.time() + 90
    while time.time() < deadline:
        resp = client.post("/api/v1/decisions", json={"trace_ids": ids})
        decisions = {d["trace_id"]: d for d in resp.json()["decisions"]}
        if all(d["status"] in ("kept", "dropped") for d in decisions.values()):
            break
        time.sleep(1)

    assert decisions["err-0"]["decision"] == "kept"
    assert decisions["err-0"]["reason"] == "error"

    kept = [d for d in decisions.values() if d["decision"] == "kept"]
    dropped = [d for d in decisions.values() if d["decision"] == "dropped"]
    # 1 error keep consumes one of the 5 slots -> 4 hash keeps, 8 drops.
    assert len(kept) == 5
    assert len(dropped) == 8
    assert all(d["reason"] == "budget_exhausted" for d in dropped)
    assert sorted(d["reason"] for d in kept).count("hashed") == 4

    # Budget endpoint reports the finalized minute.
    budgets = client.get("/api/v1/budgets").json()["budgets"]
    final = [b for b in budgets if b["finalized"]]
    assert final and final[0]["error_kept"] == 1
    assert final[0]["normal_kept"] == 4

    # Trace detail endpoint works and returns spans.
    detail = client.get("/api/v1/traces/ok-000").json()
    assert detail["span_count"] == 1
    assert detail["spans"][0]["attributes"] == {"k": "v"}
    assert client.get("/api/v1/traces/missing").status_code == 404


def test_validation_errors(client):
    # end before start
    bad = _span("bad")
    bad["start_time"] = 100
    bad["end_time"] = 1
    assert client.post("/api/v1/ingest", json={"spans": [bad]}).status_code == 422

    # bad status
    bad = _span("bad2")
    bad["status"] = "MAYBE"
    assert client.post("/api/v1/ingest", json={"spans": [bad]}).status_code == 422

    # empty batch
    assert client.post("/api/v1/ingest", json={"spans": []}).status_code == 422
