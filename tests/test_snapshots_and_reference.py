"""ticker / funding_rate / open_interest fan-out, order_book, markets, has and exchanges."""

from __future__ import annotations

import pyarrow as pa
import pytest
from vgi.client import Client

from tests.conftest import batch_count, scan

THREE = pa.scalar(["binance", "coinbase", "kraken"], pa.list_(pa.string()))


def test_ticker_single_exchange(client: Client) -> None:
    (row,) = scan(client, "ticker", "binance", "BTC/USDT").to_pylist()
    assert row["exchange"] == "binance"
    assert (row["bid"], row["last"], row["ask"]) == (39999.5, 40000.0, 40000.5)
    assert row["error"] is None


def test_ticker_fans_out_and_reports_failures_per_exchange(client: Client) -> None:
    rows = {r["exchange"]: r for r in scan(client, "ticker", THREE, "BTC/USDT").to_pylist()}
    assert list(rows) == ["binance", "coinbase", "kraken"]  # caller's order
    assert rows["binance"]["error"] is None
    assert "ExchangeNotAvailable" in rows["coinbase"]["error"]
    assert "BadSymbol" in rows["kraken"]["error"]
    assert rows["kraken"]["last"] is None


def test_ticker_fan_out_deduplicates_ids(client: Client) -> None:
    ids = pa.scalar(["okx", "OKX", "okx"], pa.list_(pa.string()))
    assert scan(client, "ticker", ids, "BTC/USDT").column("exchange").to_pylist() == ["okx"]


def test_ticker_unknown_exchange_fails_at_bind(client: Client) -> None:
    with pytest.raises(Exception, match="unknown exchange 'mtgox'"):
        scan(client, "ticker", pa.scalar(["binance", "mtgox"], pa.list_(pa.string())), "BTC/USDT")


def test_ticker_empty_exchange_list_is_an_error(client: Client) -> None:
    with pytest.raises(Exception, match="exchange list is empty"):
        scan(client, "ticker", pa.scalar([], pa.list_(pa.string())), "BTC/USDT")


def test_funding_rate_current_with_unsupported_exchange_row(client: Client) -> None:
    ids = pa.scalar(["binance", "kraken"], pa.list_(pa.string()))
    rows = {r["exchange"]: r for r in scan(client, "funding_rate", ids, "BTC/USDT:USDT").to_pylist()}
    assert rows["binance"]["funding_rate"] == 0.0001
    assert rows["binance"]["interval_hours"] == 8.0
    assert rows["binance"]["funding_rate_annualized"] == pytest.approx(0.0001 * 3 * 365)
    assert "does not support fetchFundingRate" in rows["kraken"]["error"]


def test_open_interest(client: Client) -> None:
    (row,) = scan(client, "open_interest", "okx", "BTC/USDT:USDT").to_pylist()
    assert row["open_interest_amount"] == 12_345.0
    assert row["open_interest_value"] == 12_345.0 * 40020


def test_order_book_levels_and_depth(client: Client) -> None:
    table = scan(client, "order_book", "kraken", "BTC/USD", depth=3)
    rows = [(r["side"], r["level"], r["price"], r["amount"]) for r in table.to_pylist()]
    assert rows == [
        ("bid", 1, 40999.5, 1.0),
        ("bid", 2, 40999.0, 2.0),
        ("bid", 3, 40998.5, 3.0),
        ("ask", 1, 41000.5, 1.0),
        ("ask", 2, 41001.0, 2.0),
        ("ask", 3, 41001.5, 3.0),
    ]


def test_order_book_depth_is_bounded(client: Client) -> None:
    with pytest.raises(Exception):  # noqa: B017 - SDK wording for constraint errors varies
        scan(client, "order_book", "kraken", "BTC/USD", depth=0)


def test_markets_stream_in_batches(client: Client) -> None:
    table = scan(client, "markets", "binance")
    assert table.num_rows == 2503
    assert batch_count(client, "markets", "binance") == 3
    swap = next(r for r in table.to_pylist() if r["symbol"] == "BTC/USDT:USDT")
    assert (swap["type"], swap["settle"], swap["linear"], swap["contract_size"]) == ("swap", "USDT", True, 1.0)


def test_has_reports_status_and_worker_exposure(client: Client) -> None:
    rows = {r["method"]: r for r in scan(client, "has", "binance").to_pylist()}
    assert rows["fetchOHLCV"]["supported"] and rows["fetchOHLCV"]["exposed_by_worker"]
    # Private methods appear in CCXT's map but the worker never exposes them.
    for private in ("fetchBalance", "createOrder", "fetchMyTrades", "fetchFundingHistory", "withdraw"):
        assert rows[private]["exposed_by_worker"] is False
    kraken = {r["method"]: r for r in scan(client, "has", "kraken").to_pylist()}
    assert kraken["fetchOpenInterest"]["status"] == "unknown"
    assert kraken["fetchFundingRateHistory"]["status"] == "unsupported"


def test_exchanges_lists_fixture_exchanges(client: Client) -> None:
    table = scan(client, "exchanges")
    assert table.column("id").to_pylist() == ["binance", "coinbase", "kraken", "okx"]
    funding = [r["id"] for r in table.to_pylist() if r["has_funding_rate_history"]]
    assert funding == ["binance", "okx"]
