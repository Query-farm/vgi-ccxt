"""ohlcv, trades and funding_rates: since-cursor paging against the fixture exchanges."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest
from vgi.client import Client

from ccxt_worker.history import (
    OHLCV_SCHEMA,
    FundingCursor,
    FundingRates,
    FundingRatesArgs,
    Ohlcv,
    OhlcvArgs,
    OhlcvCursor,
    Trades,
    TradesArgs,
    TradesCursor,
)
from tests.conftest import batch_count, scan

JAN1 = dt.datetime(2024, 1, 1)
T0: Any = JAN1  # Arg dataclasses take AnyArrowValue; the parser also accepts raw datetimes
HOUR = dt.timedelta(hours=1)


def ts(value: dt.datetime) -> pa.Scalar[Any]:
    return pa.scalar(value, pa.timestamp("ms"))


# ----------------------------------------------------------------------------- ohlcv


def test_ohlcv_pages_through_the_window_without_gaps_or_repeats(client: Client) -> None:
    # Binance's fixture caps a page at 100 candles; 30 days of 1h candles is 720 = 8 pages.
    table = scan(client, "ohlcv", "binance", "BTC/USDT", "1h", since=ts(JAN1), until=ts(JAN1 + 720 * HOUR))
    times: list[Any] = table.column("ts").to_pylist()
    assert len(times) == 720
    assert times == [JAN1 + i * HOUR for i in range(720)]
    assert table.schema.field("ts").type == pa.timestamp("us")


def test_ohlcv_streams_one_batch_per_exchange_page(client: Client) -> None:
    since, until = pa.scalar(JAN1, pa.timestamp("ms")), pa.scalar(JAN1 + 250 * HOUR, pa.timestamp("ms"))
    assert batch_count(client, "ohlcv", "binance", "BTC/USDT", "1h", since=since.as_py(), until=until.as_py()) == 3


def test_ohlcv_values_follow_the_fixture_formula(client: Client) -> None:
    table = scan(client, "ohlcv", "binance", "BTC/USDT", "1d", since=ts(JAN1), until=ts(JAN1 + dt.timedelta(days=3)))
    # k-th candle: open = 40000 + k % 50, close = open + (k % 7) - 3.
    assert table.column("open").to_pylist() == [40000.0, 40001.0, 40002.0]
    assert table.column("close").to_pylist() == [39997.0, 39999.0, 40001.0]


def test_ohlcv_stops_at_the_end_of_the_data(client: Client) -> None:
    # The fixture's clock is 2024-03-01; asking for more ends on an empty page.
    table = scan(client, "ohlcv", "binance", "BTC/USDT", "1d", since=ts(dt.datetime(2024, 2, 20)))
    assert table.num_rows == 10
    assert table.column("ts").to_pylist()[-1] == dt.datetime(2024, 2, 29)


def test_ohlcv_window_after_the_data_is_empty(client: Client) -> None:
    table = scan(
        client,
        "ohlcv",
        "binance",
        "BTC/USDT",
        "1d",
        since=ts(dt.datetime(2025, 1, 1)),
        until=ts(dt.datetime(2025, 2, 1)),
    )
    assert table.num_rows == 0


def test_ohlcv_terminates_when_the_exchange_ignores_since(client: Client) -> None:
    # Kraken's fixture always returns its oldest candles, whatever `since` says.
    table = scan(client, "ohlcv", "kraken", "BTC/USD", "1h", since=ts(dt.datetime(2024, 2, 26)))
    times: list[Any] = table.column("ts").to_pylist()
    assert len(times) == len(set(times)) == 96  # Feb 26 .. Feb 29, hourly; nothing repeated
    assert min(times) == dt.datetime(2024, 2, 26)


def test_ohlcv_without_since_returns_the_latest_page(client: Client) -> None:
    table = scan(client, "ohlcv", "binance", "BTC/USDT", "1h")
    assert table.num_rows == 100
    assert table.column("ts").to_pylist()[-1] == dt.datetime(2024, 2, 29, 23)


def test_ohlcv_max_rows(client: Client) -> None:
    table = scan(client, "ohlcv", "binance", "BTC/USDT", "1m", since=ts(JAN1), max_rows=250)
    assert table.num_rows == 250


def test_ohlcv_rejects_an_unoffered_timeframe(client: Client) -> None:
    with pytest.raises(Exception, match="does not offer timeframe '5m'"):
        scan(client, "ohlcv", "kraken", "BTC/USD", "5m")


def test_ohlcv_rejects_an_unknown_exchange(client: Client) -> None:
    with pytest.raises(Exception, match="unknown exchange 'notanexchange'"):
        scan(client, "ohlcv", "notanexchange", "BTC/USD", "1h")


def test_ohlcv_rejects_an_inverted_window(client: Client) -> None:
    with pytest.raises(Exception, match="since must be earlier than until"):
        scan(client, "ohlcv", "binance", "BTC/USDT", "1h", since=ts(JAN1), until=ts(JAN1 - HOUR))


def test_a_start_after_now_is_empty_not_an_error(client: Client) -> None:
    # The fixture clock is 2026-01-01; with no `until`, a later start has nothing yet.
    table = scan(client, "ohlcv", "binance", "BTC/USDT", "1h", since=ts(dt.datetime(2026, 6, 1)))
    assert table.num_rows == 0
    table = scan(client, "funding_rates", "okx", "BTC/USDT:USDT", start_date=ts(dt.datetime(2026, 6, 1)))
    assert table.num_rows == 0


def test_ohlcv_unknown_symbol_names_the_exchange(client: Client) -> None:
    with pytest.raises(Exception, match="binance fetchOHLCV failed: BadSymbol"):
        scan(client, "ohlcv", "binance", "DOGE/EUR", "1h", since=ts(JAN1))


def test_ohlcv_accepts_dates_and_iso_strings(client: Client) -> None:
    a = scan(client, "ohlcv", "binance", "BTC/USDT", "1h", since=dt.date(2024, 1, 1), until="2024-01-02T00:00:00Z")
    assert a.num_rows == 24


# ------------------------------------------------- cursor survives a process change


class Collector:
    """Stands in for the SDK's OutputCollector."""

    def __init__(self) -> None:
        self.batches: list[pa.RecordBatch] = []
        self.logs: list[str] = []
        self.finished = False

    def emit(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)

    def finish(self) -> None:
        self.finished = True

    def client_log(self, level: Any, message: str, **extra: str) -> None:
        self.logs.append(message)


def _drive(fn: Any, args: Any, schema: pa.Schema, cursor_type: Any) -> tuple[list[pa.RecordBatch], int]:
    """Run ``fn`` tick by tick, serializing the cursor between ticks as the HTTP transport does."""
    params = cast(Any, SimpleNamespace(args=args, output_schema=schema))
    blob = fn.initial_state(params).serialize_to_bytes()
    out = Collector()
    ticks = 0
    while not out.finished:
        ticks += 1
        assert ticks < 1000, "the cursor never terminated"
        state = cursor_type.deserialize_from_bytes(blob)  # a fresh process every tick
        fn.process(params, state, out)
        blob = state.serialize_to_bytes()
    return out.batches, ticks


def test_ohlcv_cursor_survives_serialization_between_pages() -> None:
    args = OhlcvArgs(exchange="binance", symbol="BTC/USDT", timeframe="1h", since=T0, until=T0 + 450 * HOUR)
    batches, ticks = _drive(Ohlcv, args, OHLCV_SCHEMA, OhlcvCursor)
    times = [t for b in batches for t in b.column("ts").to_pylist()]
    assert times == [JAN1 + i * HOUR for i in range(450)]
    assert len(batches) == 5
    assert ticks == 6  # five pages, then finish


def test_trades_cursor_survives_serialization_between_pages() -> None:
    args = TradesArgs(exchange="binance", symbol="BTC/USDT", since=T0, until=T0 + HOUR)
    batches, _ = _drive(Trades, args, Trades.FIXED_SCHEMA, TradesCursor)
    ids = [i for b in batches for i in b.column("trade_id").to_pylist()]
    assert len(batches) > 1
    assert len(ids) == len(set(ids)) == 120  # minutes 0..59 with 1, 2, 3, 1, 2, 3 ... fills


def test_funding_cursor_survives_serialization_between_pages() -> None:
    args = FundingRatesArgs(
        exchange="binance",
        symbol="BTC/USDT:USDT",
        start_date=T0,
        end_date=cast(Any, dt.datetime(2024, 2, 1)),
    )
    batches, _ = _drive(FundingRates, args, FundingRates.FIXED_SCHEMA, FundingCursor)
    assert len(batches) > 1
    rows = [r for b in batches for r in b.to_pylist()]
    assert len(rows) == 31 * 3
    assert {r["interval_hours"] for r in rows} == {8.0}


# ----------------------------------------------------------------------------- trades


def test_trades_never_drop_or_repeat_fills_sharing_a_millisecond(client: Client) -> None:
    # Pages hold 40 fills and minutes hold 1-3 fills, so pages end mid-millisecond.
    table = scan(client, "trades", "binance", "BTC/USDT", since=ts(JAN1), until=ts(JAN1 + 2 * HOUR))
    ids: list[Any] = table.column("trade_id").to_pylist()
    expected = [f"{m}-{j}" for m in range(120) for j in range(m % 3 + 1)]
    assert sorted(ids) == sorted(expected)
    assert len(ids) == len(set(ids))


def test_trades_on_an_exchange_that_needs_until_page_by_window(client: Client) -> None:
    # The coinbase fixture refuses `since` without `until` (as Coinbase does) and returns the
    # newest 50 fills of a window; the worker falls back to explicit, self-sizing windows.
    table = scan(client, "trades", "coinbase", "BTC/USD", since=ts(JAN1), until=ts(JAN1 + 3 * HOUR))
    ids: list[Any] = table.column("trade_id").to_pylist()
    expected = [f"{m}-{j}" for m in range(180) for j in range(m % 3 + 1)]
    assert sorted(ids) == sorted(expected)
    assert len(ids) == len(set(ids)) == 360


def test_windowed_trades_cursor_survives_serialization() -> None:
    args = TradesArgs(exchange="coinbase", symbol="BTC/USD", since=T0, until=T0 + 2 * HOUR)
    batches, _ = _drive(Trades, args, Trades.FIXED_SCHEMA, TradesCursor)
    ids = [i for b in batches for i in b.column("trade_id").to_pylist()]
    assert len(batches) > 1
    assert len(ids) == len(set(ids)) == 240


def test_trades_without_since_returns_recent_page(client: Client) -> None:
    table = scan(client, "trades", "binance", "BTC/USDT")
    assert 0 < table.num_rows <= 40
    assert max(cast(list[Any], table.column("ts").to_pylist())) == dt.datetime(2024, 2, 29, 23, 59)


def test_trades_max_rows(client: Client) -> None:
    table = scan(client, "trades", "binance", "BTC/USDT", since=ts(JAN1), max_rows=55)
    assert table.num_rows == 55


# ----------------------------------------------------------------------------- funding_rates


def test_funding_rates_normalize_interval_and_annualize(client: Client) -> None:
    table = scan(
        client,
        "funding_rates",
        "binance",
        "BTC/USDT:USDT",
        start_date=ts(dt.datetime(2024, 1, 31)),
        end_date=ts(dt.datetime(2024, 2, 1, 12)),
    )
    rows = table.to_pylist()
    by_ts = {r["ts"]: r for r in rows}
    # The fixture switches from 8h to 4h settlements after 2024-02-01 00:00.
    assert by_ts[dt.datetime(2024, 2, 1)]["interval_hours"] == 8.0
    assert by_ts[dt.datetime(2024, 2, 1, 4)]["interval_hours"] == 4.0
    for r in rows:
        assert r["funding_rate_annualized"] == pytest.approx(r["funding_rate"] * 8760 / r["interval_hours"])


def test_funding_rates_keep_the_sign_positive_means_longs_pay(client: Client) -> None:
    table = scan(
        client, "funding_rates", "binance", "BTC/USDT:USDT", start_date=ts(JAN1), end_date=ts(JAN1 + 72 * HOUR)
    )
    # Settlement k pays ((k % 9) - 4) / 10000: passed through unchanged, negatives included.
    assert table.column("funding_rate").to_pylist() == [((k % 9) - 4) / 10_000 for k in range(9)]


def test_funding_rates_unsupported_exchange_is_a_clear_error(client: Client) -> None:
    with pytest.raises(Exception, match="'kraken' does not support fetchFundingRateHistory"):
        scan(client, "funding_rates", "kraken", "BTC/USD")


def test_funding_rates_lone_settlement_uses_the_declared_interval(client: Client) -> None:
    table = scan(
        client,
        "funding_rates",
        "okx",
        "BTC/USDT:USDT",
        start_date=ts(dt.datetime(2024, 2, 29)),
        end_date=ts(dt.datetime(2024, 2, 29, 1)),
    )
    assert table.to_pylist()[0]["interval_hours"] == 8.0


def test_a_late_starting_history_is_reported_not_silent() -> None:
    # The kraken fixture ignores `since` and starts its candles on 2024-02-25.
    args = OhlcvArgs(exchange="kraken", symbol="BTC/USD", timeframe="1h", since=cast(Any, dt.datetime(2024, 1, 1)))
    params = cast(Any, SimpleNamespace(args=args, output_schema=OHLCV_SCHEMA))
    out = Collector()
    Ohlcv.process(params, Ohlcv.initial_state(params), cast(Any, out))
    assert out.logs and "kraken returned candles starting 2024-02-25 00:00 UTC" in out.logs[0]


def test_an_on_time_history_logs_nothing() -> None:
    args = OhlcvArgs(exchange="binance", symbol="BTC/USDT", timeframe="1h", since=T0)
    params = cast(Any, SimpleNamespace(args=args, output_schema=OHLCV_SCHEMA))
    out = Collector()
    Ohlcv.process(params, Ohlcv.initial_state(params), cast(Any, out))
    assert out.logs == []
