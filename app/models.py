"""Pydantic models for the HTTP API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


def parse_timestamp(value: Any) -> int:
    """Parse a timestamp into epoch nanoseconds.

    Accepts:
      * int/float numeric epoch; unit is inferred by magnitude (ns/us/ms/s)
      * ISO 8601 strings ("2026-09-18T10:00:00Z" or with offset)
    """
    if isinstance(value, bool):
        raise ValueError("boolean is not a valid timestamp")
    if isinstance(value, (int, float)):
        number = float(value)
        if number < 0:
            raise ValueError("timestamp must be non-negative")
        # Heuristic based on common epoch magnitudes.
        if number >= 1e17:
            return int(number)
        if number >= 1e14:
            return int(number * 1_000)
        if number >= 1e11:
            return int(number * 1_000_000)
        return int(number * 1_000_000_000)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty timestamp")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid ISO 8601 timestamp: {value!r}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    raise ValueError(f"unsupported timestamp type: {type(value).__name__}")


class SpanIn(BaseModel):
    """A single span submitted by the client."""

    trace_id: str = Field(..., min_length=1, max_length=128)
    span_id: str = Field(..., min_length=1, max_length=128)
    start_time: Any = Field(..., description="Epoch ns/us/ms/s number or ISO 8601 string")
    end_time: Any = Field(..., description="Epoch ns/us/ms/s number or ISO 8601 string")
    status: str = Field("OK", description="OK or ERROR (case-insensitive)")
    attributes: dict[str, Any] = Field(default_factory=dict)
    service_name: str | None = None
    parent_span_id: str | None = None

    @field_validator("trace_id", "span_id", "parent_span_id")
    @classmethod
    def _strip_id(cls, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value

    @field_validator("status")
    @classmethod
    def _normalize_status(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in ("OK", "ERROR"):
            raise ValueError("status must be OK or ERROR")
        return normalized

    @model_validator(mode="after")
    def _parse_times(self) -> "SpanIn":
        start_ns = parse_timestamp(self.start_time)
        end_ns = parse_timestamp(self.end_time)
        if end_ns < start_ns:
            raise ValueError("end_time must be greater than or equal to start_time")
        object.__setattr__(self, "start_time", start_ns)
        object.__setattr__(self, "end_time", end_ns)
        return self

    model_config = {"extra": "ignore"}


class BatchIngestIn(BaseModel):
    spans: list[SpanIn] = Field(..., min_length=1, max_length=10_000)
