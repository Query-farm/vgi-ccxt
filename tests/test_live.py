"""Opt-in live smoke check against real public endpoints: ``make live-check``.

Skipped unless ``VGI_CCXT_LIVE=1``. Every request is an unauthenticated public GET
to Kraken, Coinbase or OKX, issued through the worker with CCXT's rate limiter on,
and the whole module makes a few dozen requests. Values move, so the assertions
are about shape and invariants, not numbers.
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest
from vgi.client import Client

from tests.conftest import scan

pytestmark = pytest.mark.skipif(os.environ.get("VGI_CCXT_LIVE") != "1", reason="set VGI_CCXT_LIVE=1 to run")

DAY = dt.timedelta(days=1)


@pytest.fixture(scope="module")
def live() -> Iterator[Client]:
    """The worker with fixtures switched off (the session conftest turns them on)."""
    with Client(["env", "-u", "VGI_CCXT_FIXTURES", sys.executable, "-m", "ccxt_worker.worker"]) as c:
        yield c


def ts(value: dt.datetime) -> pa.Scalar[Any]:
    return pa.scalar(value, pa.timestamp("ms"))


def ids(*names: str) -> pa.Scalar[Any]:
    return pa.scalar(list(names), pa.list_(pa.string()))


def test_exchanges_is_ccxt_catalog(live: Client) -> None:
    table = scan(live, "exchanges")
    assert table.num_rows > 100
    assert {"kraken", "coinbase", "okx"} <= set(table.column("id").to_pylist())


def test_ohlcv_multi_page_backfill_coinbase(live: Client) -> None:
    # Coinbase caps a page at 300 candles: two weeks of 1h candles needs two pages.
    start = dt.datetime(2025, 1, 1)
    table = scan(live, "ohlcv", "coinbase", "BTC/USD", "1h", since=ts(start), until=ts(start + 14 * DAY))
    times: list[Any] = table.column("ts").to_pylist()
    assert len(times) == len(set(times)) == 336
    assert times == sorted(times)
    lows: list[Any] = table.column("low").to_pylist()
    highs: list[Any] = table.column("high").to_pylist()
    assert all(lo <= hi for lo, hi in zip(lows, highs, strict=True))


def test_ohlcv_okx_week(live: Client) -> None:
    start = dt.datetime(2025, 1, 1)
    table = scan(live, "ohlcv", "okx", "ETH/USDT", "1h", since=ts(start), until=ts(start + 7 * DAY))
    assert table.num_rows == 168


def test_ohlcv_kraken_recent_only(live: Client) -> None:
    # Kraken serves only its latest 720 candles, so a recent window works and an old one is empty.
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    recent = scan(live, "ohlcv", "kraken", "BTC/USD", "1h", since=ts(now - 2 * DAY))
    assert 40 <= recent.num_rows <= 50
    old = scan(
        live, "ohlcv", "kraken", "BTC/USD", "1h", since=ts(dt.datetime(2025, 1, 1)), until=ts(dt.datetime(2025, 1, 8))
    )
    assert old.num_rows == 0


def test_ticker_fan_out(live: Client) -> None:
    rows = {r["exchange"]: r for r in scan(live, "ticker", ids("kraken", "coinbase"), "BTC/USD").to_pylist()}
    for exchange in ("kraken", "coinbase"):
        assert rows[exchange]["error"] is None, rows[exchange]["error"]
        assert rows[exchange]["bid"] <= rows[exchange]["ask"]
    assert abs(rows["kraken"]["last"] / rows["coinbase"]["last"] - 1) < 0.02


def test_order_book_kraken(live: Client) -> None:
    table = scan(live, "order_book", "kraken", "BTC/USD", depth=10)
    rows = table.to_pylist()
    bids = [r["price"] for r in rows if r["side"] == "bid"]
    asks = [r["price"] for r in rows if r["side"] == "ask"]
    assert len(bids) == len(asks) == 10
    assert bids == sorted(bids, reverse=True) and asks == sorted(asks)
    assert bids[0] < asks[0]


def test_trades_recent_kraken(live: Client) -> None:
    table = scan(live, "trades", "kraken", "BTC/USD")
    ids_: list[Any] = table.column("trade_id").to_pylist()
    assert table.num_rows > 0 and len(ids_) == len(set(ids_))


def test_trades_coinbase_windowed(live: Client) -> None:
    # Coinbase rejects `since` without `until`; the worker walks explicit windows.
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    table = scan(live, "trades", "coinbase", "BTC/USD", since=ts(now - dt.timedelta(minutes=10)))
    ids_: list[Any] = table.column("trade_id").to_pylist()
    assert table.num_rows > 0 and len(ids_) == len(set(ids_))
    times: list[Any] = table.column("ts").to_pylist()
    assert min(times) >= now - dt.timedelta(minutes=10, seconds=1)


def test_markets_okx_perpetuals(live: Client) -> None:
    table = scan(live, "markets", "okx")
    swaps = {r["symbol"]: r for r in table.to_pylist() if r["type"] == "swap"}
    assert swaps["BTC/USDT:USDT"]["linear"] is True
    assert table.num_rows > 1000


def test_funding_rates_okx_recent(live: Client) -> None:
    start = dt.datetime.now(dt.UTC).replace(tzinfo=None) - 20 * DAY
    table = scan(live, "funding_rates", "okx", "BTC/USDT:USDT", start_date=ts(start))
    rows = table.to_pylist()
    assert 55 <= len(rows) <= 65  # 8h settlements
    assert {r["interval_hours"] for r in rows} == {8.0}
    for r in rows:
        assert r["funding_rate_annualized"] == pytest.approx(r["funding_rate"] * 3 * 365)
        assert abs(r["funding_rate"]) < 0.01


def test_funding_rate_and_open_interest_okx(live: Client) -> None:
    (funding,) = scan(live, "funding_rate", "okx", "BTC/USDT:USDT").to_pylist()
    assert funding["error"] is None and funding["interval_hours"] == 8.0
    assert funding["next_funding_ts"] > dt.datetime.now(dt.UTC).replace(tzinfo=None)
    (oi,) = scan(live, "open_interest", "okx", "BTC/USDT:USDT").to_pylist()
    assert oi["error"] is None and oi["open_interest_amount"] > 0


def test_unsupported_capability_is_a_row_error(live: Client) -> None:
    (row,) = scan(live, "funding_rate", "kraken", "BTC/USD").to_pylist()
    assert "does not support fetchFundingRate" in row["error"]
