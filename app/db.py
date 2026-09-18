"""SQLite schema and connection management."""

from __future__ import annotations

import os

import aiosqlite

from .config import settings

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS traces (
    trace_id         TEXT PRIMARY KEY,
    min_start_ns     INTEGER NOT NULL,
    max_end_ns       INTEGER NOT NULL,
    duration_ms      REAL NOT NULL,
    has_error        INTEGER NOT NULL DEFAULT 0,
    is_slow          INTEGER NOT NULL DEFAULT 0,
    span_count       INTEGER NOT NULL DEFAULT 0,
    service_names    TEXT NOT NULL DEFAULT '',
    -- open / pending / kept / dropped
    status           TEXT NOT NULL DEFAULT 'open',
    -- error | slow | hashed | budget_exhausted
    reason           TEXT,
    budget_minute    INTEGER,
    hash_value       INTEGER,
    closed_at_ns     INTEGER,
    decided_at_ns    INTEGER,
    updated_at_ns    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_traces_close ON traces(status, updated_at_ns);
CREATE INDEX IF NOT EXISTS idx_traces_minute ON traces(budget_minute, status);

CREATE TABLE IF NOT EXISTS spans (
    span_id        TEXT NOT NULL,
    trace_id       TEXT NOT NULL,
    start_ns       INTEGER NOT NULL,
    end_ns         INTEGER NOT NULL,
    status         TEXT NOT NULL,
    service_name   TEXT,
    parent_span_id TEXT,
    attributes     TEXT NOT NULL,
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id);

CREATE TABLE IF NOT EXISTS budget_minutes (
    minute_ns       INTEGER PRIMARY KEY,  -- UTC minute start, epoch nanoseconds
    error_kept      INTEGER NOT NULL DEFAULT 0,
    slow_kept       INTEGER NOT NULL DEFAULT 0,
    normal_kept     INTEGER NOT NULL DEFAULT 0,
    finalized       INTEGER NOT NULL DEFAULT 0
);
"""


async def connect() -> aiosqlite.Connection:
    """Open the SQLite database and make sure the schema exists."""
    os.makedirs(os.path.dirname(os.path.abspath(settings.db_path)), exist_ok=True)
    db = await aiosqlite.connect(settings.db_path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode = WAL")
    await db.execute("PRAGMA synchronous = NORMAL")
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("PRAGMA busy_timeout = 5000")
    await db.executescript(SCHEMA)
    await db.commit()
    return db
