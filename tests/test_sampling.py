"""Core sampler tests: quiet window, priority rules, per-minute hash
budget, stability and late-span reopening."""

from types import SimpleNamespace

import pytest_asyncio

from app import sampling
from app.config import settings
from app.db import connect


class FakeClock:
    def __init__(self, start: int):
        self.t = start

    def __call__(self) -> int:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += int(seconds * sampling.NS_PER_SEC)


def aligned_start() -> int:
    """A point 10 s into the current UTC minute, safely away from boundaries."""
    now = sampling.now_ns()
    minute = (now // sampling.NS_PER_MINUTE) * sampling.NS_PER_MINUTE
    return minute + 10 * sampling.NS_PER_SEC


@pytest_asyncio.fixture  # noqa: F401
async def db(tmp_path):
    object.__setattr__(settings, "db_path", str(tmp_path / "t.db"))
    conn = await connect()
    clock = FakeClock(aligned_start())
    sampling.now_ns = clock
    try:
        yield conn, clock
    finally:
        await conn.close()


def span(trace_id, idx=0, status="OK", start=0, end=50_000_000, service="svc"):
    return SimpleNamespace(
        trace_id=trace_id,
        span_id=f"{trace_id}-s{idx}",
        start_time=start,
        end_time=end,
        status=status,
        service_name=service,
        parent_span_id=None,
        attributes={"i": idx},
    )


async def status_of(conn, trace_id):
    async with conn.cursor() as cur:
        await cur.execute("SELECT * FROM traces WHERE trace_id = ?", (trace_id,))
        row = await cur.fetchone()
    return None if row is None else dict(row)


async def test_open_then_priority_kept(db):
    conn, clock = db
    await sampling.ingest_spans(conn, [span("t-err", status="ERROR")])
    assert (await status_of(conn, "t-err"))["status"] == "open"

    # Nothing closes inside the quiet window.
    clock.advance(settings.quiet_seconds / 2)
    assert await sampling.close_due_traces(conn) == 0
    assert (await status_of(conn, "t-err"))["status"] == "open"

    clock.advance(settings.quiet_seconds)
    assert await sampling.close_due_traces(conn) == 1
    row = await status_of(conn, "t-err")
    assert row["status"] == "kept"
    assert row["reason"] == "error"


async def test_slow_trace_always_kept(db):
    conn, clock = db
    await sampling.ingest_spans(
        conn,
        [span("t-slow", start=0, end=1_500 * sampling.NS_PER_MS)],
    )
    clock.advance(settings.quiet_seconds + 0.01)
    await sampling.close_due_traces(conn)
    row = await status_of(conn, "t-slow")
    assert row["status"] == "kept" and row["reason"] == "slow"


async def test_normal_trace_pending_until_minute_finalized(db):
    conn, clock = db
    await sampling.ingest_spans(conn, [span("t-ok")])
    clock.advance(settings.quiet_seconds + 0.01)
    await sampling.close_due_traces(conn)
    assert (await status_of(conn, "t-ok"))["status"] == "pending"

    # Minute not finalized yet (still inside its quiet window) -> stays pending.
    await sampling.finalize_due_minutes(conn)
    assert (await status_of(conn, "t-ok"))["status"] == "pending"

    # Advance past (minute end + quiet window); the clock starts at +10s
    # of its minute, so +55s lands at +65s of the original minute.
    clock.advance(55)
    await sampling.finalize_due_minutes(conn)
    row = await status_of(conn, "t-ok")
    assert row["status"] == "kept" and row["reason"] == "hashed"


async def test_budget_exhaustion_and_stable_hash_order(db):
    conn, clock = db
    budget = settings.sample_budget_per_minute
    n = budget + 8
    ids = [f"trace-{i:03d}" for i in range(n)]
    await sampling.ingest_spans(conn, [span(tid) for tid in ids])

    clock.advance(settings.quiet_seconds + 0.01)
    await sampling.close_due_traces(conn)
    clock.advance(61)
    await sampling.finalize_due_minutes(conn)

    kept, dropped = [], []
    for tid in ids:
        row = await status_of(conn, tid)
        (kept if row["status"] == "kept" else dropped).append(tid)
    assert len(kept) == budget
    assert len(dropped) == 8
    for tid in dropped:
        assert (await status_of(conn, tid))["reason"] == "budget_exhausted"

    # Exactly the budget smallest stable hashes win.
    ranked = sorted(ids, key=lambda t: (sampling.stable_hash(t), t))
    assert set(kept) == set(ranked[:budget])


async def test_decisions_are_reproducible(db, tmp_path):
    """Running the same batch from scratch must yield identical decisions."""
    original_now = sampling.now_ns

    async def run(run_id: int):
        object.__setattr__(settings, "db_path", str(tmp_path / f"rep{run_id}.db"))
        conn = await connect()
        # Identical wall-clock position in both runs.
        base = aligned_start() + run_id * 10 * sampling.NS_PER_MINUTE
        fake = FakeClock(base)
        sampling.now_ns = fake
        ids = [f"rep-{i:03d}" for i in range(20)]
        await sampling.ingest_spans(conn, [span(tid) for tid in ids])
        fake.advance(settings.quiet_seconds + 0.01)
        await sampling.close_due_traces(conn)
        fake.advance(61)
        await sampling.finalize_due_minutes(conn)
        out = {}
        for tid in ids:
            row = await status_of(conn, tid)
            out[tid] = (row["status"], row["reason"])
        await conn.close()
        return out

    try:
        first = await run(1)
        second = await run(2)
    finally:
        sampling.now_ns = original_now
    assert first == second


async def test_idempotent_batch_ingest(db):
    conn, clock = db
    s = span("dup")
    inserted1, tids1 = await sampling.ingest_spans(conn, [s])
    inserted2, tids2 = await sampling.ingest_spans(conn, [s])
    assert inserted1 == 1 and inserted2 == 0
    assert tids1 == tids2 == ["dup"]
    row = await status_of(conn, "dup")
    assert row["span_count"] == 1


async def test_late_span_reopens_decided_trace(db):
    conn, clock = db
    await sampling.ingest_spans(conn, [span("late", idx=0, status="OK")])
    clock.advance(settings.quiet_seconds + 0.01)
    await sampling.close_due_traces(conn)
    clock.advance(61)
    await sampling.finalize_due_minutes(conn)
    row = await status_of(conn, "late")
    assert row["status"] == "kept"

    # A late ERROR span arrives: trace reopens and is later re-kept as error.
    clock.advance(5)
    await sampling.ingest_spans(
        conn, [span("late", idx=1, status="ERROR", start=1, end=90_000_000)]
    )
    row = await status_of(conn, "late")
    assert row["status"] == "open" and row["has_error"] == 1
    clock.advance(settings.quiet_seconds + 0.01)
    await sampling.close_due_traces(conn)
    row = await status_of(conn, "late")
    assert row["status"] == "kept" and row["reason"] == "error"


async def test_error_rule_outranks_slow(db):
    conn, clock = db
    await sampling.ingest_spans(
        conn,
        [
            span(
                "both",
                status="ERROR",
                start=0,
                end=5_000 * sampling.NS_PER_MS,
            )
        ],
    )
    clock.advance(settings.quiet_seconds + 0.01)
    await sampling.close_due_traces(conn)
    row = await status_of(conn, "both")
    assert row["status"] == "kept" and row["reason"] == "error"


async def test_priority_traces_consume_minute_budget(db):
    conn, clock = db
    budget = settings.sample_budget_per_minute
    # 3 error traces + budget normal traces in the same minute.
    spans = [span(f"e{i}", status="ERROR") for i in range(3)]
    spans += [span(f"n{i:03d}") for i in range(budget)]
    await sampling.ingest_spans(conn, spans)
    clock.advance(settings.quiet_seconds + 0.01)
    await sampling.close_due_traces(conn)
    clock.advance(61)
    await sampling.finalize_due_minutes(conn)

    normal_kept = [
        tid
        for tid in [f"n{i:03d}" for i in range(budget)]
        if (await status_of(conn, tid))["status"] == "kept"
    ]
    # Only budget - 3 hash slots remain for normal traffic.
    assert len(normal_kept) == budget - 3
