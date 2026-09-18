"""Unit tests for timestamp parsing and stable hashing."""

import pytest

from app.models import parse_timestamp
from app.sampling import stable_hash


def test_numeric_unit_inference():
    assert parse_timestamp(1_700_000_000) == 1_700_000_000 * 1_000_000_000
    assert parse_timestamp(1_700_000_000_000) == 1_700_000_000_000 * 1_000_000
    assert parse_timestamp(1_700_000_000_000_000) == 1_700_000_000_000_000 * 1_000
    ns = 1_700_000_000_000_000_000
    assert parse_timestamp(ns) == ns
    assert parse_timestamp(1.5) == 1_500_000_000


def test_iso8601_parsing():
    assert parse_timestamp("2026-09-18T10:00:00Z") == parse_timestamp(
        "2026-09-18T10:00:00+00:00"
    )
    # Naive timestamps are treated as UTC.
    assert parse_timestamp("2026-09-18T10:00:00") == parse_timestamp(
        "2026-09-18T10:00:00+00:00"
    )
    # Offset is honored.
    assert parse_timestamp("2026-09-18T11:00:00+01:00") == parse_timestamp(
        "2026-09-18T10:00:00Z"
    )


@pytest.mark.parametrize("value", [-1, "not-a-time", "", True])
def test_invalid_timestamps(value):
    with pytest.raises(ValueError):
        parse_timestamp(value)


def test_stable_hash_is_deterministic_and_bounded():
    h1 = stable_hash("abc123")
    h2 = stable_hash("abc123")
    assert h1 == h2
    assert 0 <= h1 < 1 << 63
    assert stable_hash("abc124") != h1
