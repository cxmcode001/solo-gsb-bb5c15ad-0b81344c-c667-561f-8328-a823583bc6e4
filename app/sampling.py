"""Tail sampling core: span ingestion, quiet-window aggregation and the
per-minute hash budget sampler.

Sampling rules, in priority order:
  1. traces containing an ERROR span are always kept (reason=error)
  2. traces at least TS_SLOW_THRESHOLD_MS long are always kept (reason=slow)
  3. remaining "normal" traces are kept by a stable hash of trace_id
     (reason=hashed), filling only the remaining per-minute budget;
     overflow is dropped deterministically (reason=budget_exhausted).

Decisions are stable: a trace's hash never changes, and for any fixed set
of candidate traces in a minute the kept/dropped set is identical on
every run.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import aiosqlite

from .config import settings

NS_PER_MS = 1_000_000
NS_PER_SEC = 1_000_000_000
NS_PER_MINUTE = 60 * NS_PER_SEC
HASH_SPACE = 1 << 63


def now_ns() -> int:
    return time.time_ns()


def stable_hash(trace_id: str) -> int:
    """Deterministic 63-bit hash of a trace id."""
    digest = hashlib.sha256(trace_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & (HASH_SPACE - 1)


def minute_bucket_ns(timestamp_ns: int) -> int:
    return (timestamp_ns // NS_PER_MINUTE) * NS_PER_MINUTE


def _service_names(rows: list[aiosqlite.Row]) -> str:
    names = sorted(
        {row["service_name"] for row in rows if row["service_name"]}
    )
    return json.dumps(names, ensure_ascii=False)


async def ingest_spans(
    db: aiosqlite.Connection, spans: list[Any]
) -> tuple[int, list[str]]:
    """Insert a batch of spans (idempotent on (trace_id, span_id)) and
    refresh the aggregated trace records.

    Returns (new_span_count, affected_trace_ids).
    """
    now = now_ns()
    async with db.cursor() as cur:
        await cur.executemany(
            """
            INSERT OR IGNORE INTO spans
                (span_id, trace_id, start_ns, end_ns, status,
                 service_name, parent_span_id, attributes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    span.span_id,
                    span.trace_id,
                    span.start_time,
                    span.end_time,
                    span.status,
                    span.service_name,
                    span.parent_span_id,
                    json.dumps(span.attributes, ensure_ascii=False),
                )
                for span in spans
            ],
        )
        inserted = cur.rowcount or 0
        trace_ids = sorted({span.trace_id for span in spans})

        for trace_id in trace_ids:
            await cur.execute(
                """
                SELECT MIN(start_ns)  AS min_start,
                       MAX(end_ns)    AS max_end,
                       SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
                       COUNT(*)       AS span_count
                FROM spans WHERE trace_id = ?
                """,
                (trace_id,),
            )
            agg = await cur.fetchone()
            min_start = agg["min_start"]
            max_end = agg["max_end"]
            span_count = agg["span_count"]
            duration_ms = max(0.0, (max_end - min_start) / NS_PER_MS)
            has_error = 1 if (agg["errors"] or 0) > 0 else 0
            is_slow = 1 if duration_ms >= settings.slow_threshold_ms else 0

            await cur.execute(
                "SELECT service_names, status, budget_minute, reason FROM traces "
                "WHERE trace_id = ?",
                (trace_id,),
            )
            existing = await cur.fetchone()

            if existing is None:
                await cur.execute(
                    """
                    INSERT INTO traces (trace_id, min_start_ns, max_end_ns,
                                        duration_ms, has_error, is_slow,
                                        span_count, service_names, status,
                                        updated_at_ns)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
                    """,
                    (
                        trace_id, min_start, max_end, duration_ms,
                        has_error, is_slow, span_count,
                        _service_names(await _trace_span_rows(cur, trace_id)),
                        now,
                    ),
                )
                continue

            services = _service_names(await _trace_span_rows(cur, trace_id))
            old_status = existing["status"]
            old_minute = existing["budget_minute"]

            if old_status == "open":
                await cur.execute(
                    """
                    UPDATE traces
                       SET min_start_ns = ?, max_end_ns = ?, duration_ms = ?,
                           has_error = ?, is_slow = ?, span_count = ?,
                           service_names = ?, updated_at_ns = ?
                     WHERE trace_id = ?
                    """,
                    (
                        min_start, max_end, duration_ms, has_error, is_slow,
                        span_count, services, now, trace_id,
                    ),
                )
            else:
                # A late span arrived after the quiet window (or even after
                # a decision). Reopen the trace; the scheduler re-closes it
                # after a fresh quiet window and re-evaluates it.
                await cur.execute(
                    """
                    UPDATE traces
                       SET min_start_ns = ?, max_end_ns = ?, duration_ms = ?,
                           has_error = ?, is_slow = ?, span_count = ?,
                           service_names = ?, status = 'open', reason = NULL,
                           budget_minute = NULL, decided_at_ns = NULL,
                           closed_at_ns = NULL, updated_at_ns = ?
                     WHERE trace_id = ?
                    """,
                    (
                        min_start, max_end, duration_ms, has_error, is_slow,
                        span_count, services, now, trace_id,
                    ),
                )
                # Give back the budget slot previously consumed by a kept
                # trace, but only while that minute is not yet finalized.
                # Pending traces never consumed a slot; dropped traces never
                # did either.
                if old_status == "kept" and old_minute is not None:
                    column = {
                        "error": "error_kept",
                        "slow": "slow_kept",
                        "hashed": "normal_kept",
                    }[existing["reason"]]
                    await cur.execute(
                        f"UPDATE budget_minutes SET {column} = MAX(0, {column} - 1) "
                        "WHERE minute_ns = ? AND finalized = 0",
                        (old_minute,),
                    )
    await db.commit()
    return inserted, trace_ids


async def _trace_span_rows(
    cur: aiosqlite.Cursor, trace_id: str
) -> list[aiosqlite.Row]:
    await cur.execute(
        "SELECT service_name FROM spans WHERE trace_id = ?", (trace_id,)
    )
    return list(await cur.fetchall())


async def close_due_traces(db: aiosqlite.Connection) -> int:
    """Close open traces whose quiet window has elapsed.

    Error/slow traces are decided immediately. Normal traces become pending
    and wait for their budget minute to be finalized.
    """
    now = now_ns()
    cutoff = now - int(settings.quiet_seconds * NS_PER_SEC)
    closed = 0

    async with db.cursor() as cur:
        await cur.execute(
            "SELECT trace_id, has_error, is_slow, updated_at_ns FROM traces "
            "WHERE status = 'open' AND updated_at_ns <= ?",
            (cutoff,),
        )
        due = list(await cur.fetchall())
        if not due:
            return 0

        for row in due:
            trace_id = row["trace_id"]
            # Charge the trace to the UTC minute in which its quiet window
            # began (i.e. its last span arrived), not to the current minute:
            # traces that went quiet together share one sampling budget.
            minute = minute_bucket_ns(row["updated_at_ns"])
            await cur.execute(
                """
                INSERT INTO budget_minutes (minute_ns) VALUES (?)
                ON CONFLICT(minute_ns) DO NOTHING
                """,
                (minute,),
            )
            if row["has_error"]:
                await cur.execute(
                    """
                    UPDATE traces
                       SET status = 'kept', reason = 'error',
                           budget_minute = ?, closed_at_ns = ?,
                           decided_at_ns = ?, updated_at_ns = ?
                     WHERE trace_id = ?
                    """,
                    (minute, now, now, now, trace_id),
                )
                await cur.execute(
                    "UPDATE budget_minutes SET error_kept = error_kept + 1 "
                    "WHERE minute_ns = ?",
                    (minute,),
                )
            elif row["is_slow"]:
                await cur.execute(
                    """
                    UPDATE traces
                       SET status = 'kept', reason = 'slow',
                           budget_minute = ?, closed_at_ns = ?,
                           decided_at_ns = ?, updated_at_ns = ?
                     WHERE trace_id = ?
                    """,
                    (minute, now, now, now, trace_id),
                )
                await cur.execute(
                    "UPDATE budget_minutes SET slow_kept = slow_kept + 1 "
                    "WHERE minute_ns = ?",
                    (minute,),
                )
            else:
                await cur.execute(
                    """
                    UPDATE traces
                       SET status = 'pending', budget_minute = ?,
                           hash_value = ?, closed_at_ns = ?, updated_at_ns = ?
                     WHERE trace_id = ?
                    """,
                    (minute, stable_hash(trace_id), now, now, trace_id),
                )
            closed += 1
    await db.commit()
    return closed


async def finalize_due_minutes(db: aiosqlite.Connection) -> int:
    """Make hash decisions for every budget minute past its quiet window."""
    now = now_ns()
    # A minute can be finalized once it has completely ended AND the quiet
    # window after its end has elapsed (traces closing near the end of the
    # minute may still receive late spans).
    cutoff = minute_bucket_ns(
        now - int(settings.quiet_seconds * NS_PER_SEC) - NS_PER_MINUTE
    )
    finalized = 0

    async with db.cursor() as cur:
        await cur.execute(
            "SELECT minute_ns, error_kept, slow_kept FROM budget_minutes "
            "WHERE finalized = 0 AND minute_ns <= ? ORDER BY minute_ns",
            (cutoff,),
        )
        minutes = list(await cur.fetchall())

        for minute_row in minutes:
            minute_ns = minute_row["minute_ns"]
            budget = max(
                0,
                settings.sample_budget_per_minute
                - minute_row["error_kept"]
                - minute_row["slow_kept"],
            )

            await cur.execute(
                "SELECT trace_id FROM traces "
                "WHERE budget_minute = ? AND status = 'pending' "
                "ORDER BY hash_value ASC, trace_id ASC",
                (minute_ns,),
            )
            candidates = list(await cur.fetchall())

            keep_ids = [r["trace_id"] for r in candidates[:budget]]
            drop_ids = [r["trace_id"] for r in candidates[budget:]]

            for ids, status, reason in (
                (keep_ids, "kept", "hashed"),
                (drop_ids, "dropped", "budget_exhausted"),
            ):
                for i in range(0, len(ids), 900):
                    chunk = ids[i : i + 900]
                    placeholders = ",".join("?" for _ in chunk)
                    await cur.execute(
                        f"UPDATE traces SET status = ?, reason = ?, decided_at_ns = ? "
                        f"WHERE trace_id IN ({placeholders})",
                        [status, reason, now, *chunk],
                    )

            if keep_ids:
                await cur.execute(
                    "UPDATE budget_minutes SET normal_kept = normal_kept + ? "
                    "WHERE minute_ns = ?",
                    (len(keep_ids), minute_ns),
                )
            await cur.execute(
                "UPDATE budget_minutes SET finalized = 1 WHERE minute_ns = ?",
                (minute_ns,),
            )
            finalized += 1
    await db.commit()
    return finalized
