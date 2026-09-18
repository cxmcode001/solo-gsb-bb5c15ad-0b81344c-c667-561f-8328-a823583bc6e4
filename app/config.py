"""Runtime configuration loaded from environment variables.

All settings have usable defaults, so the service can be started without
any third-party account or external infrastructure.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    db_path: str = os.getenv("TS_DB_PATH", "data/tailsampling.db")
    host: str = os.getenv("TS_HOST", "0.0.0.0")
    port: int = _env_int("TS_PORT", 8080)

    # A trace is considered complete when no span has arrived for this long.
    quiet_seconds: float = _env_float("TS_QUIET_SECONDS", 20.0)
    # Keep budget for ordinary (non-error, non-slow) traces per UTC minute.
    sample_budget_per_minute: int = _env_int("TS_SAMPLE_BUDGET_PER_MINUTE", 100)
    # Traces at least this long are classified as slow and always kept.
    slow_threshold_ms: int = _env_int("TS_SLOW_THRESHOLD_MS", 1000)
    # Background scheduler interval.
    tick_seconds: float = _env_float("TS_TICK_SECONDS", 1.0)


settings = Settings()
