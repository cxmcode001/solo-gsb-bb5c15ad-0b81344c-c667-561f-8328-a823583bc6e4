"""Pytest configuration: point the service at a temp database and use
fast scheduler settings before the app modules are imported."""

import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="tailsampling-test-")
os.environ.setdefault("TS_DB_PATH", os.path.join(_tmp, "test.db"))
os.environ.setdefault("TS_QUIET_SECONDS", "0.05")
os.environ.setdefault("TS_SAMPLE_BUDGET_PER_MINUTE", "5")
os.environ.setdefault("TS_SLOW_THRESHOLD_MS", "1000")
os.environ.setdefault("TS_TICK_SECONDS", "0.02")
