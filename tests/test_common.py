"""Argument parsing and funding normalization helpers."""

from __future__ import annotations

import datetime as dt

import pytest

from ccxt_worker.common import (
    annualize,
    interval_hours_from_gap,
    interval_hours_from_label,
    num,
    time_arg_ms,
)

HOUR_MS = 3_600_000


def test_time_arg_naive_is_utc() -> None:
    assert time_arg_ms(dt.datetime(2024, 1, 1), "since") == 1_704_067_200_000


def test_time_arg_aware_and_strings_and_dates() -> None:
    eastern = dt.timezone(dt.timedelta(hours=-5))
    assert time_arg_ms(dt.datetime(2023, 12, 31, 19, tzinfo=eastern), "since") == 1_704_067_200_000
    assert time_arg_ms("2024-01-01T00:00:00Z", "since") == 1_704_067_200_000
    assert time_arg_ms(dt.date(2024, 1, 1), "since") == 1_704_067_200_000
    assert time_arg_ms(None, "since") is None


def test_time_arg_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="since must be a timestamp"):
        time_arg_ms("yesterday", "since")


@pytest.mark.parametrize(("gap", "hours"), [(8 * HOUR_MS, 8.0), (HOUR_MS + 3, 1.0), (4 * HOUR_MS - 60_000, 4.0)])
def test_interval_from_standard_gaps(gap: int, hours: float) -> None:
    assert interval_hours_from_gap(gap) == hours


@pytest.mark.parametrize("gap", [None, 0, -HOUR_MS, 16 * HOUR_MS, 3 * HOUR_MS, int(8.5 * HOUR_MS)])
def test_interval_rejects_data_gaps(gap: int | None) -> None:
    assert interval_hours_from_gap(gap) is None


def test_interval_labels() -> None:
    assert interval_hours_from_label("8h") == 8.0
    assert interval_hours_from_label("1d") == 24.0
    assert interval_hours_from_label(None) is None
    assert interval_hours_from_label("soon") is None


def test_annualize() -> None:
    assert annualize(0.0001, 8.0) == pytest.approx(0.1095)
    assert annualize(-0.0001, 1.0) == pytest.approx(-0.876)
    assert annualize(None, 8.0) is None
    assert annualize(0.0001, None) is None


def test_num() -> None:
    assert num("1.5") == 1.5
    assert num(float("nan")) is None
    assert num(True) is None
    assert num("x") is None
