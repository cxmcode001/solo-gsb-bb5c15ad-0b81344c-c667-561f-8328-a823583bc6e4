"""Read-side queries used by the HTTP API."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from .sampling import NS_PER_MS


def _iso_minute(minute_ns: int) -> str:
    return datetime.fromtimestamp(minute_ns / 1e9, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:00Z"
    )


def serialize_decision(row: aiosqlite.Row | None, trace_id: str) -> dict[str, Any]:
    if row is None:
        return {
            "trace_id": trace_id,
            "decision": "unknown",
            "retained": None,
            "reason": "trace_not_found",
            "status": "unknown",
        }
    return {
        "trace_id": trace_id,
        "decision": row["status"],
        "retained": row["status"] == "kept" if row["status"] in ("kept", "dropped") else None,
        "reason": row["reason"],
        "status": row["status"],
        "duration_ms": round(row["duration_ms"], 3),
        "has_error": bool(row["has_error"]),
        "is_slow": bool(row["is_slow"]),
        "span_count": row["span_count"],
        "budget_minute": _iso_minute(row["budget_minute"]) if row["budget_minute"] is not None else None,
        "hash_value": row["hash_value"],
    }


async def get_decisions(
    db: aiosqlite.Connection, trace_ids: list[str]
) -> list[dict[str, Any]]:
    if not trace_ids:
        return []
    result: list[dict[str, Any]] = []
    async with db.cursor() as cur:
        for i in range(0, len(trace_ids), 900):
            chunk = trace_ids[i : i + 900]
            placeholders = ",".join("?" for _ in chunk)
            await cur.execute(
                f"SELECT * FROM traces WHERE trace_id IN ({placeholders})", chunk
            )
            rows = {r["trace_id"]: r for r in await cur.fetchall()}
            result.extend(
                serialize_decision(rows.get(tid), tid) for tid in chunk
            )
    return result


async def list_decisions(
    db: aiosqlite.Connection,
    status: str | None,
    minute_ns: int | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    where: list[str] = []
    params: list[Any] = []
    if status:
        where.append("status = ?")
        params.append(status)
    if minute_ns is not None:
        where.append("budget_minute = ?")
        params.append(minute_ns)
    clause = ("WHERE " + " AND ".join(where)) if where else ""

    async with db.cursor() as cur:
        await cur.execute(f"SELECT COUNT(*) AS c FROM traces {clause}", params)
        total = (await cur.fetchone())["c"]
        await cur.execute(
            f"SELECT * FROM traces {clause} ORDER BY updated_at_ns DESC, trace_id "
            f"LIMIT ? OFFSET ?",
            (*params, limit, offset),
        )
        rows = list(await cur.fetchall())
    return [serialize_decision(r, r["trace_id"]) for r in rows], total


async def get_trace_detail(
    db: aiosqlite.Connection, trace_id: str
) -> dict[str, Any] | None:
    async with db.cursor() as cur:
        await cur.execute("SELECT * FROM traces WHERE trace_id = ?", (trace_id,))
        trace = await cur.fetchone()
        if trace is None:
            return None
        await cur.execute(
            "SELECT span_id, parent_span_id, start_ns, end_ns, status, "
            "service_name, attributes FROM spans WHERE trace_id = ? "
            "ORDER BY start_ns ASC, span_id ASC",
            (trace_id,),
        )
        spans = []
        for row in await cur.fetchall():
            spans.append(
                {
                    "span_id": row["span_id"],
                    "parent_span_id": row["parent_span_id"],
                    "start_ns": row["start_ns"],
                    "end_ns": row["end_ns"],
                    "duration_ms": round(
                        max(0, row["end_ns"] - row["start_ns"]) / NS_PER_MS, 3
                    ),
                    "status": row["status"],
                    "service_name": row["service_name"],
                    "attributes": json.loads(row["attributes"]),
                }
            )
    detail = serialize_decision(trace, trace_id)
    detail["min_start_ns"] = trace["min_start_ns"]
    detail["max_end_ns"] = trace["max_end_ns"]
    detail["service_names"] = json.loads(trace["service_names"] or "[]")
    detail["spans"] = spans
    return detail


async def list_budgets(
    db: aiosqlite.Connection,
    minute_ns: int | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    where = "WHERE minute_ns = ?" if minute_ns is not None else ""
    params: list[Any] = [minute_ns] if minute_ns is not None else []
    async with db.cursor() as cur:
        await cur.execute(f"SELECT COUNT(*) AS c FROM budget_minutes {where}", params)
        total = (await cur.fetchone())["c"]
        await cur.execute(
            f"SELECT * FROM budget_minutes {where} ORDER BY minute_ns DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        )
        rows = list(await cur.fetchall())
    items = []
    for r in rows:
        items.append(
            {
                "minute": _iso_minute(r["minute_ns"]),
                "minute_ns": r["minute_ns"],
                "error_kept": r["error_kept"],
                "slow_kept": r["slow_kept"],
                "normal_kept": r["normal_kept"],
                "priority_kept": r["error_kept"] + r["slow_kept"],
                "total_kept": r["error_kept"] + r["slow_kept"] + r["normal_kept"],
                "finalized": bool(r["finalized"]),
            }
        )
    return items, total
