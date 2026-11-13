"""Shared argument parsing and value normalization for the table functions."""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Callable
from typing import Any

import pyarrow as pa
from vgi.arguments import AnyArrowValue

from .client import validate_exchange_id

HOUR_MS = 3_600_000

#: Types accepted for a point-in-time argument: TIMESTAMP, TIMESTAMPTZ, DATE or an ISO-8601 string.
TIME_TYPES: list[Callable[[pa.DataType], bool]] = [
    pa.types.is_timestamp,
    pa.types.is_date,
    pa.types.is_string,
    pa.types.is_large_string,
]

#: Types accepted for an exchange argument: one id, or a list of ids for a fan-out.
EXCHANGE_TYPES: list[Callable[[pa.DataType], bool]] = [
    pa.types.is_string,
    pa.types.is_large_string,
    pa.types.is_list,
    pa.types.is_large_list,
]

MAX_FANOUT = 50

#: Funding intervals (hours) exchanges actually use; a gap outside this set is a data gap.
STANDARD_FUNDING_HOURS = (1, 2, 4, 8, 12, 24)
HOURS_PER_YEAR = 24 * 365

#: Output timestamps are microsecond Arrow timestamps, which DuckDB reads as plain `TIMESTAMP`.
TIMESTAMP = pa.timestamp("us")


def us(ms: int | None) -> int | None:
    """Convert epoch milliseconds (CCXT's unit) to the microseconds ``TIMESTAMP`` holds."""
    return None if ms is None else ms * 1000


def _unwrap(value: Any) -> Any:
    return value.value if isinstance(value, AnyArrowValue) else value


def time_arg_ms(value: Any, name: str) -> int | None:
    """Convert a TIMESTAMP / TIMESTAMPTZ / DATE / ISO string argument to epoch ms (UTC).

    A timestamp without a time zone is taken to be UTC. ``None`` stays ``None``.
    """
    raw = _unwrap(value)
    if raw is None:
        return None
    if isinstance(raw, str):
        text = raw.strip()
        try:
            raw = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"{name} must be a timestamp or an ISO-8601 string, got {text!r}") from None
    if isinstance(raw, dt.datetime):
        moment = raw if raw.tzinfo is not None else raw.replace(tzinfo=dt.UTC)
        return int(moment.timestamp() * 1000)
    if isinstance(raw, dt.date):
        return int(dt.datetime(raw.year, raw.month, raw.day, tzinfo=dt.UTC).timestamp() * 1000)
    raise ValueError(f"{name} must be a TIMESTAMP, DATE or ISO-8601 string")


def exchange_list(value: Any) -> list[str]:
    """Validate an exchange argument (one id or a list of ids) and return unique ids in order."""
    raw = _unwrap(value)
    items = raw if isinstance(raw, list) else [raw]
    if not items:
        raise ValueError("exchange list is empty; pass at least one CCXT exchange id")
    ids: list[str] = []
    for item in items:
        exchange_id = validate_exchange_id(item)
        if exchange_id not in ids:
            ids.append(exchange_id)
    if len(ids) > MAX_FANOUT:
        raise ValueError(f"at most {MAX_FANOUT} exchanges per call, got {len(ids)}")
    return ids


def num(value: Any) -> float | None:
    """Return a finite float, or ``None`` for missing / non-numeric / NaN values."""
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def int_or_none(value: Any) -> int | None:
    """Return an int, or ``None`` for missing / non-numeric values."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def interval_hours_from_gap(gap_ms: int | None) -> float | None:
    """Return the funding interval implied by the gap between two settlements, if standard."""
    if gap_ms is None or gap_ms <= 0:
        return None
    hours = round(gap_ms / HOUR_MS)
    if hours in STANDARD_FUNDING_HOURS and abs(gap_ms - hours * HOUR_MS) <= 5 * 60_000:
        return float(hours)
    return None


def interval_hours_from_label(label: Any) -> float | None:
    """Parse CCXT's funding ``interval`` label (``'8h'``, ``'1h'``, ``'1d'``) to hours."""
    if not isinstance(label, str) or len(label) < 2:
        return None
    unit, amount = label[-1], label[:-1]
    if not amount.isdigit():
        return None
    factor = {"h": 1, "d": 24, "m": 1 / 60}.get(unit)
    return float(int(amount) * factor) if factor else None


def annualize(rate: float | None, interval_hours: float | None) -> float | None:
    """Annualize a per-interval funding rate (simple, not compounded)."""
    if rate is None or not interval_hours:
        return None
    return rate * HOURS_PER_YEAR / interval_hours
