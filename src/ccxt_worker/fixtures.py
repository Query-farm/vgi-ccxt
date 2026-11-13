"""A deterministic, offline stand-in for a CCXT exchange, driven by a JSON fixture file.

Enabled only when ``VGI_CCXT_FIXTURES`` names a fixture file (see ``client.py``).
The unit tests, the SQL tests over every transport and the stock-DuckDB
verification all run against it, so none of them touches the network.

The stand-in mimics the CCXT behaviour the worker depends on:

* ``fetch_ohlcv`` / ``fetch_trades`` / ``fetch_funding_rate_history`` honour
  ``since`` and cap each page at the fixture's per-call limit, so long windows
  need several pages, exactly like a real exchange.
* Values are computed from the row's index, so tests can assert exact sums.
* Trades share timestamps (several fills in one millisecond), so a page can end
  in the middle of a millisecond; the cursor must neither drop nor repeat them.
* A fixture can make a method raise a named CCXT error class, ignore ``since``
  (``ohlcv_ignores_since``), change the funding interval part-way, or end a
  series' history (``end``) before the fixture clock (``now``).

This module does not import ``ccxt``: the caller passes the CCXT module in as
``errors`` so raised errors are real CCXT exception classes.
"""

from __future__ import annotations

import datetime as dt
import functools
import json
from pathlib import Path
from typing import Any, cast

HOUR_MS = 3_600_000
_TIMEFRAMES_S = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14_400, "1d": 86_400}


def iso_ms(value: str) -> int:
    """Parse an ISO-8601 UTC timestamp to epoch milliseconds."""
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return int(parsed.timestamp() * 1000)


@functools.cache
def load_fixture(path: str) -> dict[str, Any]:
    """Read and normalize a fixture file (cached per path for the process lifetime)."""
    data: dict[str, Any] = json.loads(Path(path).read_text())
    data["now_ms"] = iso_ms(data["now"])
    return data


def _iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).isoformat().replace("+00:00", "Z")


class FakeExchange:
    """One fixture exchange, exposing the attributes and public methods CCXT exchanges have."""

    def __init__(self, exchange_id: str, fixture: dict[str, Any], *, errors: Any) -> None:
        """Build the stand-in for ``exchange_id`` from ``fixture``; ``errors`` is the ccxt module."""
        spec = fixture["exchanges"][exchange_id]
        self._spec = spec
        self._errors = errors
        self._now = int(fixture["now_ms"])
        self.id = exchange_id
        self.name = spec["name"]
        self.countries = spec.get("countries", [])
        self.certified = spec.get("certified", False)
        self.pro = spec.get("pro", False)
        self.rateLimit = spec.get("rateLimit", 100)
        self.urls = {"www": spec.get("www")}
        self.has = spec["has"]
        self.timeframes = {tf: tf for tf in spec.get("timeframes", [])}
        limits = spec.get("limits", {})
        self.features = {"spot": {"fetchOHLCV": {"limit": limits.get("ohlcv", 100)}}}
        self._limits = limits
        self._series: dict[str, Any] = spec.get("series", {})

    # -------------------------------------------------------------- helpers

    def _fail(self, method: str) -> None:
        error = self._spec.get("errors", {}).get(method)
        if error:
            raise getattr(self._errors, error["class"])(f"{self.id} {error['message']}")

    def _series_for(self, symbol: str) -> dict[str, Any]:
        series = self._series.get(symbol)
        if series is None:
            raise self._errors.BadSymbol(f"{self.id} does not have market symbol {symbol}")
        return cast(dict[str, Any], series)

    def _end(self, series: dict[str, Any]) -> int:
        """Where a series' history stops: its own ``end``, else the fixture clock."""
        return iso_ms(series["end"]) if "end" in series else self._now

    def _page(self, method: str, limit: int | None) -> int:
        cap = int(self._limits.get(method, 100))
        return min(cap, limit) if limit else cap

    # -------------------------------------------------------------- markets

    def load_markets(self) -> dict[str, Any]:
        """Markets by symbol: the fixture's own plus ``generate_markets`` synthetic spot pairs."""
        self._fail("fetchMarkets")
        markets: dict[str, Any] = {}
        for market in self._spec.get("markets", []):
            markets[market["symbol"]] = {
                "id": market["symbol"].replace("/", "").split(":")[0],
                "spot": market["type"] == "spot",
                "margin": False,
                "swap": market["type"] == "swap",
                "future": market["type"] == "future",
                "option": False,
                "contract": market["type"] != "spot",
                "active": True,
                "precision": {"price": 0.01, "amount": 0.0001},
                "limits": {"amount": {"min": 0.0001}, "cost": {"min": 5.0}},
                "maker": 0.001,
                "taker": 0.002,
                **market,
            }
        for i in range(int(self._spec.get("generate_markets", 0))):
            symbol = f"GEN{i:04d}/USDT"
            markets[symbol] = {
                "id": f"GEN{i:04d}USDT",
                "symbol": symbol,
                "base": f"GEN{i:04d}",
                "quote": "USDT",
                "type": "spot",
                "spot": True,
                "active": i % 10 != 0,
                "maker": 0.001,
                "taker": 0.001,
                "precision": {"price": 0.0001, "amount": 1.0},
                "limits": {"amount": {"min": 1.0}, "cost": {"min": 1.0}},
            }
        return markets

    # -------------------------------------------------------------- candles

    def fetch_ohlcv(
        self, symbol: str, timeframe: str = "1m", since: int | None = None, limit: int | None = None
    ) -> list[list[float]]:
        """Candles for ``symbol``: ``[ts, open, high, low, close, volume]``, oldest first."""
        self._fail("fetchOHLCV")
        series = self._series_for(symbol)
        tf_ms = _TIMEFRAMES_S[timeframe] * 1000
        start = iso_ms(series["start"])
        total = max(0, (self._end(series) - start) // tf_ms)
        count = self._page("ohlcv", limit)
        if since is None or self._spec.get("ohlcv_ignores_since"):
            first = max(0, total - count) if since is None else 0
        else:
            first = max(0, -(-(since - start) // tf_ms))
        rows: list[list[float]] = []
        price = float(series["price"])
        for k in range(first, min(total, first + count)):
            open_ = price + k % 50
            close = open_ + (k % 7) - 3
            rows.append([start + k * tf_ms, open_, max(open_, close) + 2, min(open_, close) - 2, close, 10.0 + k % 10])
        return rows

    # -------------------------------------------------------------- trades

    def fetch_trades(self, symbol: str, since: int | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        """Trades every minute, 1-3 fills sharing each minute's timestamp, oldest first."""
        self._fail("fetchTrades")
        series = self._series_for(symbol)
        start = iso_ms(series["start"])
        minutes = max(0, (self._end(series) - start) // 60_000)
        count = self._page("trades", limit)
        everything: list[dict[str, Any]] = []
        first_minute = 0 if since is None else max(0, -(-(since - start) // 60_000))
        if since is None:
            first_minute = max(0, minutes - count)
        price = float(series["price"])
        for m in range(first_minute, minutes):
            ts = start + m * 60_000
            for j in range(m % 3 + 1):
                everything.append(
                    {
                        "id": f"{m}-{j}",
                        "timestamp": ts,
                        "datetime": _iso(ts),
                        "symbol": symbol,
                        "side": "buy" if (m + j) % 2 == 0 else "sell",
                        "takerOrMaker": "taker",
                        "price": price + m % 20,
                        "amount": 1.0 + j,
                        "cost": (price + m % 20) * (1.0 + j),
                    }
                )
            if since is not None and len(everything) >= count:
                break
        return everything[:count] if since is not None else everything[-count:]

    # -------------------------------------------------------------- snapshots

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        """Return a fixed ticker built from the series price."""
        self._fail("fetchTicker")
        price = float(self._series_for(symbol)["price"])
        return {
            "symbol": symbol,
            "timestamp": self._now,
            "datetime": _iso(self._now),
            "last": price,
            "close": price,
            "bid": price - 0.5,
            "ask": price + 0.5,
            "high": price * 1.02,
            "low": price * 0.98,
            "open": price * 0.99,
            "baseVolume": 1000.0,
            "quoteVolume": 1000.0 * price,
            "percentage": 1.0101,
            "vwap": price * 1.001,
        }

    def fetch_order_book(self, symbol: str, limit: int | None = None) -> dict[str, Any]:
        """``limit`` levels a side (default 100), 0.5 apart, level ``n`` sized ``n``."""
        self._fail("fetchOrderBook")
        price = float(self._series_for(symbol)["price"])
        depth = limit or 100
        return {
            "symbol": symbol,
            "timestamp": self._now,
            "bids": [[price - 0.5 * n, float(n)] for n in range(1, depth + 1)],
            "asks": [[price + 0.5 * n, float(n)] for n in range(1, depth + 1)],
            "nonce": None,
        }

    # -------------------------------------------------------------- derivatives

    def _funding_times(self, series: dict[str, Any]) -> list[int]:
        times: list[int] = []
        ts = iso_ms(series["start"])
        hours = int(series.get("funding_hours", 8))
        change = series.get("funding_change")
        change_ms = iso_ms(change["at"]) if change else None
        end = self._end(series)
        while ts < end:
            times.append(ts)
            if change_ms is not None and ts >= change_ms and change is not None:
                hours = int(change["hours"])
            ts += hours * HOUR_MS
        return times

    def fetch_funding_rate_history(
        self, symbol: str, since: int | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Settled funding: ``((k % 9) - 4) / 10000`` at settlement ``k``, oldest first."""
        self._fail("fetchFundingRateHistory")
        series = self._series_for(symbol)
        if "funding_hours" not in series:
            raise self._errors.BadSymbol(f"{self.id} {symbol} is not a perpetual swap")
        count = self._page("funding", limit)
        rows = [
            {"symbol": symbol, "fundingRate": ((k % 9) - 4) / 10_000, "timestamp": ts, "datetime": _iso(ts)}
            for k, ts in enumerate(self._funding_times(series))
            if since is None or ts >= since
        ]
        return rows[:count] if since is not None else rows[-count:]

    def fetch_funding_rate(self, symbol: str) -> dict[str, Any]:
        """Return the upcoming settlement: rate 0.0001 per ``funding_hours`` interval."""
        self._fail("fetchFundingRate")
        series = self._series_for(symbol)
        if "funding_hours" not in series:
            raise self._errors.BadSymbol(f"{self.id} {symbol} is not a perpetual swap")
        hours = int(series.get("funding_hours", 8))
        nxt = (self._now // (hours * HOUR_MS) + 1) * hours * HOUR_MS
        return {
            "symbol": symbol,
            "timestamp": self._now,
            "fundingRate": 0.0001,
            "fundingTimestamp": nxt,
            "markPrice": float(series["price"]) + 1.0,
            "indexPrice": float(series["price"]),
            "interval": f"{hours}h",
        }

    def fetch_open_interest(self, symbol: str) -> dict[str, Any]:
        """Open interest of 12,345 contracts at the series price."""
        self._fail("fetchOpenInterest")
        series = self._series_for(symbol)
        return {
            "symbol": symbol,
            "timestamp": self._now,
            "openInterestAmount": 12_345.0,
            "openInterestValue": 12_345.0 * float(series["price"]),
        }
