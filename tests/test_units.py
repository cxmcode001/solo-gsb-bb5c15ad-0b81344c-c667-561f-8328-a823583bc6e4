"""Unit tests for timestamp normalization and stable hashing."""
from __future__ import annotations

import pytest

from app.timeutil import minute_bucket, to_ns
from app.sampler import hash_score, select_by_hash, stable_hash
from app.models import SpanIn
from pydantic import ValidationError


def test_epoch_units_are_normalized_to_ns():
    assert to_ns(1_700_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000.5) == 1_700_000_000 * 10**9 + 500_000_000


def test_iso8601_z_and_offsets():
    assert to_ns("2023-11-14T22:13:20Z") == 1_700_000_000 * 10**9
    assert to_ns("2023-11-14T23:13:20+01:00") == 1_700_000_000 * 10**9
    # Naive timestamps are interpreted as UTC.
    assert to_ns("2023-11-14T22:13:20") == 1_700_000_000 * 10**9


@pytest.mark.parametrize("bad", ["not-a-time", "2026-13-99", None])
def test_bad_timestamps_raise(bad):
    with pytest.raises((ValueError, TypeError)):
        to_ns(bad)


def test_minute_bucket_aligns_downward():
    base = 1_700_000_040  # epoch seconds, known multiple of 60
    assert minute_bucket((base + 59) * 10**9) == base
    assert minute_bucket((base + 60) * 10**9) == base + 60


def test_hash_is_stable_and_spread():
    assert stable_hash("abc") == stable_hash("abc")
    assert stable_hash("abc") != stable_hash("abd")
    scores = [hash_score(f"trace-{i}") for i in range(2000)]
    assert min(scores) >= 0 and max(scores) < 1
    # Rough uniformity: roughly half on each side of 0.5.
    below = sum(1 for s in scores if s < 0.5)
    assert 800 < below < 1200


def test_span_invalid_timestamp_is_rejected():
    with pytest.raises(ValidationError):
        SpanIn(trace_id="t", span_id="s", start_time="nope", end_time=1)
    # end before start is schema-valid but rejected per-span by the engine.
    span = SpanIn(trace_id="t", span_id="s", start_time=10, end_time=5)
    assert span.trace_id == "t"


def test_select_by_hash_exact_counts_and_stability():
    ids = [f"t{i}" for i in range(20)]
    kept1, dropped1 = select_by_hash(ids, 7)
    kept2, dropped2 = select_by_hash(list(reversed(ids)), 7)
    assert len(kept1) == 7 and len(dropped1) == 13
    # Input order must not change which traces win.
    assert set(kept1) == set(kept2)
    assert set(dropped1) == set(dropped2)
    # Zero budget drops everyone; oversized budget keeps everyone.
    kept, dropped = select_by_hash(ids, 0)
    assert kept == [] and len(dropped) == 20
    kept, dropped = select_by_hash(ids, 100)
    assert len(kept) == 20 and dropped == []
