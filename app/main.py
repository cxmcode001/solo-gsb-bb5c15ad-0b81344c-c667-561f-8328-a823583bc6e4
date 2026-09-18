"""FastAPI application: batch ingest + decision query APIs.

Endpoints (prefix /api/v1):
  POST /ingest                       batch submit spans
  POST /decisions                    batch query decisions by trace id
  GET  /decisions                    list/filter decisions
  GET  /traces/{trace_id}            full aggregated trace with spans
  GET  /budgets                      per-minute budget usage
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
from fastapi import Body, FastAPI, HTTPException, Query

from . import queries
from .config import settings
from .db import connect
from .models import BatchIngestIn, parse_timestamp
from .sampling import (
    close_due_traces,
    finalize_due_minutes,
    ingest_spans,
    minute_bucket_ns,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("tail-sampler")

_db: aiosqlite.Connection | None = None
_scheduler_task: asyncio.Task | None = None
# Created during startup so it binds to the running event loop.
_write_lock: asyncio.Lock | None = None


async def _scheduler() -> None:
    """Background loop: close quiet traces, then finalize due minutes."""
    while True:
        try:
            assert _db is not None and _write_lock is not None
            async with _write_lock:
                closed = await close_due_traces(_db)
                finalized = await finalize_due_minutes(_db)
            if closed or finalized:
                logger.info(
                    "scheduler: closed=%d traces, finalized=%d minutes",
                    closed,
                    finalized,
                )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - keep the loop alive
            logger.exception("scheduler tick failed")
        await asyncio.sleep(settings.tick_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db, _scheduler_task, _write_lock
    _write_lock = asyncio.Lock()
    _db = await connect()
    _scheduler_task = asyncio.create_task(_scheduler())
    logger.info(
        "tail sampler started db=%s quiet=%.1fs budget/min=%d slow>=%dms",
        settings.db_path,
        settings.quiet_seconds,
        settings.sample_budget_per_minute,
        settings.slow_threshold_ms,
    )
    try:
        yield
    finally:
        if _scheduler_task is not None:
            _scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await _scheduler_task
        if _db is not None:
            await _db.close()
        _db = None


app = FastAPI(
    title="Distributed Trace Tail Sampling Service",
    version="1.0.0",
    description=(
        "Aggregates spans into traces over a quiet window, always keeps "
        "error/slow traces, and fills the per-minute budget with ordinary "
        "traces selected by a stable hash."
    ),
    lifespan=lifespan,
)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "trace-tail-sampler",
        "version": "1.0.0",
        "quiet_seconds": settings.quiet_seconds,
        "sample_budget_per_minute": settings.sample_budget_per_minute,
        "slow_threshold_ms": settings.slow_threshold_ms,
        "docs": "/docs",
    }


@app.get("/health")
async def health() -> dict[str, str]:
    assert _db is not None
    await _db.execute("SELECT 1")
    return {"status": "ok"}


@app.post("/api/v1/ingest", status_code=202)
async def ingest(batch: BatchIngestIn) -> dict[str, Any]:
    assert _db is not None and _write_lock is not None
    async with _write_lock:
        inserted, trace_ids = await ingest_spans(_db, batch.spans)
    return {
        "accepted_spans": len(batch.spans),
        "new_spans": inserted,
        "duplicate_spans": len(batch.spans) - inserted,
        "trace_count": len(trace_ids),
        "trace_ids": trace_ids,
        "status": "accepted",
    }


@app.post("/api/v1/decisions")
async def decisions_by_ids(
    trace_ids: list[str] = Body(..., embed=True, min_length=1, max_length=10_000),
) -> dict[str, Any]:
    assert _db is not None
    items = await queries.get_decisions(_db, trace_ids)
    return {"count": len(items), "decisions": items}


@app.get("/api/v1/decisions")
async def list_decisions(
    status: str | None = Query(
        None,
        description="open | pending | kept | dropped",
        pattern="^(open|pending|kept|dropped)$",
    ),
    minute: str | None = Query(
        None,
        description="UTC budget minute, e.g. 2026-09-18T10:00:00Z",
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    assert _db is not None
    minute_ns = minute_bucket_ns(parse_timestamp(minute)) if minute else None
    items, total = await queries.list_decisions(
        _db, status, minute_ns, limit, offset
    )
    return {"total": total, "limit": limit, "offset": offset, "decisions": items}


@app.get("/api/v1/traces/{trace_id}")
async def trace_detail(trace_id: str) -> dict[str, Any]:
    assert _db is not None
    detail = await queries.get_trace_detail(_db, trace_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return detail


@app.get("/api/v1/budgets")
async def budgets(
    minute: str | None = Query(None, description="UTC minute, e.g. 2026-09-18T10:00:00Z"),
    limit: int = Query(60, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    assert _db is not None
    minute_ns = minute_bucket_ns(parse_timestamp(minute)) if minute else None
    items, total = await queries.list_budgets(_db, minute_ns, limit, offset)
    return {
        "total": total,
        "budget_per_minute": settings.sample_budget_per_minute,
        "budgets": items,
    }
