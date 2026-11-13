"""Snapshot table functions: ``ticker``, ``funding_rate``, ``open_interest`` and ``order_book``.

``ticker``, ``funding_rate`` and ``open_interest`` take one exchange id or a list
of them and fan out, one exchange per ``process`` tick. A failure on one
exchange (unsupported capability, unknown symbol, geo-block, outage) becomes a
row with the ``error`` column set, so one bad venue does not sink a
cross-exchange comparison. Unknown exchange ids are still rejected at bind.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import AnyArrowValue, Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import PublicExchange, get_exchange
from .common import (
    EXCHANGE_TYPES,
    MAX_FANOUT,
    TIMESTAMP,
    annualize,
    exchange_list,
    int_or_none,
    interval_hours_from_gap,
    interval_hours_from_label,
    num,
    us,
)
from .history import signed_rate
from .meta import TERMS_NOTE, column, object_tags, result_columns

_FANOUT_DOC = (
    f"One CCXT exchange id such as 'kraken', or a list of up to {MAX_FANOUT} ids to compare exchanges side by side."
)
_ERROR_DOC = "NULL on success; otherwise why this exchange returned no data (the other columns are then NULL)."


@dataclass(slots=True, frozen=True, kw_only=True)
class FanoutArgs:
    """Arguments for a fan-out snapshot: ``fn(exchange | [exchange, ...], symbol)``."""

    exchange: Annotated[AnyArrowValue, Arg(0, doc=_FANOUT_DOC, type_bound=EXCHANGE_TYPES)]
    symbol: Annotated[
        str, Arg(1, doc="CCXT unified symbol such as 'BTC/USD'; perpetual swaps look like 'BTC/USDT:USDT'.")
    ]


@dataclass(kw_only=True)
class FanoutCursor(ArrowSerializableDataclass):
    """Which exchanges to visit and how many are done; travels with each HTTP continuation."""

    exchanges: list[str] = field(default_factory=list)
    index: int = 0


Row = dict[str, Any]


def _fanout_tick(
    params: ProcessParams[FanoutArgs],
    state: FanoutCursor,
    out: OutputCollector,
    capability: str,
    fetch: Callable[[PublicExchange, str], Row],
) -> None:
    """Visit the next exchange, emitting its row or an error row."""
    if state.index >= len(state.exchanges):
        out.finish()
        return
    exchange_id = state.exchanges[state.index]
    state.index += 1
    symbol = params.args.symbol
    row: Row = {"exchange": exchange_id, "symbol": symbol, "error": None}
    try:
        exchange = get_exchange(exchange_id)
        exchange.require(capability)
        row.update(fetch(exchange, symbol))
    except Exception as err:  # reported per exchange, by design
        row["error"] = str(err) or type(err).__name__
    schema = params.output_schema
    out.emit(pa.RecordBatch.from_pydict({f.name: [row.get(f.name)] for f in schema}, schema=schema))


def _fanout_bind(params: BindParams[FanoutArgs], schema: pa.Schema) -> BindResponse:
    exchange_list(params.args.exchange)
    if not params.args.symbol.strip():
        raise ValueError("symbol must be a CCXT unified symbol such as 'BTC/USD'")
    return BindResponse(output_schema=schema)


def _fanout_state(params: ProcessParams[FanoutArgs]) -> FanoutCursor:
    return FanoutCursor(exchanges=exchange_list(params.args.exchange))


# =============================================================================
# ticker
# =============================================================================

TICKER_SCHEMA = pa.schema(
    [
        column("exchange", pa.string(), "CCXT exchange id.", nullable=False),
        column("symbol", pa.string(), "CCXT unified market symbol.", nullable=False),
        column("ts", TIMESTAMP, "When the exchange computed the quote, UTC (NULL if it does not say)."),
        column("last", pa.float64(), "Last traded price, in the quote currency."),
        column("bid", pa.float64(), "Best bid price."),
        column("ask", pa.float64(), "Best ask price."),
        column("high", pa.float64(), "Highest price over the exchange's rolling 24-hour window."),
        column("low", pa.float64(), "Lowest price over the exchange's rolling 24-hour window."),
        column("open", pa.float64(), "Price at the start of the rolling 24-hour window."),
        column("base_volume", pa.float64(), "24-hour volume in the base currency."),
        column("quote_volume", pa.float64(), "24-hour volume in the quote currency."),
        column("change_pct", pa.float64(), "24-hour price change, in percent."),
        column("vwap", pa.float64(), "24-hour volume-weighted average price, when published."),
        column("error", pa.string(), _ERROR_DOC),
    ]
)


def _ticker(exchange: PublicExchange, symbol: str) -> Row:
    t = exchange.call("fetch_ticker", symbol)
    return {
        "ts": us(int_or_none(t.get("timestamp"))),
        "last": num(t.get("last")),
        "bid": num(t.get("bid")),
        "ask": num(t.get("ask")),
        "high": num(t.get("high")),
        "low": num(t.get("low")),
        "open": num(t.get("open")),
        "base_volume": num(t.get("baseVolume")),
        "quote_volume": num(t.get("quoteVolume")),
        "change_pct": num(t.get("percentage")),
        "vwap": num(t.get("vwap")),
    }


@init_single_worker
class Ticker(TableFunctionGenerator[FanoutArgs, FanoutCursor]):
    """The current quote for one market, on one exchange or fanned out across several."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TICKER_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "ticker"
        description = "Current price, best bid/ask and 24-hour stats for a market, across one or more exchanges"
        tags = {
            **object_tags(
                title="Ticker Quotes",
                category="snapshots",
                keywords="ticker, quote, price, last price, bid ask, spread, 24h volume, cross exchange, arbitrage",
                doc_llm=(
                    "The current quote for one market: last price, best bid and ask, rolling 24-hour high, "
                    "low, open, volumes and percent change. The first argument is one exchange id "
                    "('kraken') or a list (['kraken', 'coinbase', 'okx']) for a side-by-side comparison, "
                    "one row per exchange; the second is the unified symbol ('BTC/USD'). Symbols differ by "
                    "venue: many exchanges quote in USDT ('BTC/USDT') rather than USD. If one exchange "
                    "fails (symbol not listed, geo-blocked, down) its row carries the reason in `error` "
                    "and NULL prices, and the other rows are unaffected; filter `WHERE error IS NULL`."
                ),
                doc_md=(
                    "## ticker(exchange | [exchange, ...], symbol)\n\n"
                    "One row per exchange with the current quote; failures appear in `error`.\n\n" + TERMS_NOTE
                ),
            ),
            "vgi.result_columns_schema": result_columns(TICKER_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT exchange, last, ask - bid AS spread, quote_volume "
                    "FROM ccxt.main.ticker(['kraken', 'coinbase'], 'BTC/USD') "
                    "WHERE error IS NULL ORDER BY last"
                ),
                description="Compare BTC/USD on two US exchanges",
            ),
            FunctionExample(
                sql="SELECT last, change_pct FROM ccxt.main.ticker('okx', 'ETH/USDT')",
                description="The current ETH/USDT price and 24-hour change on OKX",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FanoutArgs]) -> BindResponse:
        """Validate every exchange id up front."""
        return _fanout_bind(params, TICKER_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FanoutArgs]) -> FanoutCursor:
        """Queue the exchanges to visit."""
        return _fanout_state(params)

    @classmethod
    def process(cls, params: ProcessParams[FanoutArgs], state: FanoutCursor, out: OutputCollector) -> None:
        """Quote the next exchange."""
        _fanout_tick(params, state, out, "fetchTicker", _ticker)


# =============================================================================
# funding_rate (current)
# =============================================================================

FUNDING_NOW_SCHEMA = pa.schema(
    [
        column("exchange", pa.string(), "CCXT exchange id.", nullable=False),
        column("symbol", pa.string(), "CCXT unified perpetual symbol.", nullable=False),
        column("ts", TIMESTAMP, "When the exchange published the rate, UTC."),
        column(
            "funding_rate",
            pa.float64(),
            "Rate for the upcoming settlement as a decimal per interval; positive means longs pay shorts.",
        ),
        column("interval_hours", pa.float64(), "Funding interval in hours, as the exchange declares it."),
        column("funding_rate_annualized", pa.float64(), "funding_rate times intervals per 365-day year (simple)."),
        column("next_funding_ts", TIMESTAMP, "When the rate settles, UTC."),
        column("mark_price", pa.float64(), "Mark price used for margining."),
        column("index_price", pa.float64(), "Spot index price the perpetual tracks."),
        column("error", pa.string(), _ERROR_DOC),
    ]
)


def _funding_now(exchange: PublicExchange, symbol: str) -> Row:
    f = exchange.call("fetch_funding_rate", symbol)
    rate = signed_rate(exchange.id, f.get("fundingRate"))
    hours = interval_hours_from_label(f.get("interval"))
    if hours is None:
        nxt, prev = int_or_none(f.get("fundingTimestamp")), int_or_none(f.get("previousFundingTimestamp"))
        hours = interval_hours_from_gap(nxt - prev) if nxt is not None and prev is not None else None
    return {
        "ts": us(int_or_none(f.get("timestamp"))),
        "funding_rate": rate,
        "interval_hours": hours,
        "funding_rate_annualized": annualize(rate, hours),
        "next_funding_ts": us(int_or_none(f.get("fundingTimestamp"))),
        "mark_price": num(f.get("markPrice")),
        "index_price": num(f.get("indexPrice")),
    }


@init_single_worker
class FundingRate(TableFunctionGenerator[FanoutArgs, FanoutCursor]):
    """The current (next-settlement) funding rate of a perpetual, across one or more exchanges."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FUNDING_NOW_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "funding_rate"
        description = "Current funding rate of a perpetual swap, per interval and annualized, across exchanges"
        tags = {
            **object_tags(
                title="Current Funding Rate",
                category="derivatives",
                keywords="funding rate, current funding, predicted funding, perpetual swap, perps, mark price",
                doc_llm=(
                    "The funding rate that applies at the next settlement of a perpetual swap, with its "
                    "interval, an annualized rate, the settlement time, mark and index price. First "
                    "argument: one exchange id or a list for comparison; second: the unified perpetual "
                    "symbol ('BTC/USDT:USDT' on Binance, Bybit and OKX). `funding_rate` is a decimal per "
                    "interval with positive = longs pay shorts. Exchanges without fetchFundingRate, or that "
                    "do not list the symbol, return a row with `error` set. For settled history use "
                    "`funding_rates`."
                ),
                doc_md=(
                    "## funding_rate(exchange | [exchange, ...], symbol)\n\n"
                    "Next-settlement funding per exchange, decimal per interval (positive = longs pay) "
                    "and annualized.\n\n" + TERMS_NOTE
                ),
            ),
            "vgi.result_columns_schema": result_columns(FUNDING_NOW_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT exchange, funding_rate, interval_hours, funding_rate_annualized "
                    "FROM ccxt.main.funding_rate(['binance', 'okx'], 'BTC/USDT:USDT') "
                    "WHERE error IS NULL ORDER BY funding_rate_annualized DESC"
                ),
                description="Rank venues by annualized BTC perpetual funding",
            ),
            FunctionExample(
                sql=(
                    "SELECT funding_rate, next_funding_ts FROM ccxt.main.funding_rate('okx', 'ETH/USDT:USDT') "
                    "WHERE error IS NULL"
                ),
                description="The next ETH perpetual funding settlement on OKX",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FanoutArgs]) -> BindResponse:
        """Validate every exchange id up front."""
        return _fanout_bind(params, FUNDING_NOW_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FanoutArgs]) -> FanoutCursor:
        """Queue the exchanges to visit."""
        return _fanout_state(params)

    @classmethod
    def process(cls, params: ProcessParams[FanoutArgs], state: FanoutCursor, out: OutputCollector) -> None:
        """Fetch the next exchange's funding rate."""
        _fanout_tick(params, state, out, "fetchFundingRate", _funding_now)


# =============================================================================
# open_interest
# =============================================================================

OPEN_INTEREST_SCHEMA = pa.schema(
    [
        column("exchange", pa.string(), "CCXT exchange id.", nullable=False),
        column("symbol", pa.string(), "CCXT unified derivatives symbol.", nullable=False),
        column("ts", TIMESTAMP, "When the exchange measured open interest, UTC."),
        column(
            "open_interest_amount",
            pa.float64(),
            "Open interest in contracts or base currency, as the exchange reports it.",
        ),
        column("open_interest_value", pa.float64(), "Open interest in the quote or settlement currency."),
        column("error", pa.string(), _ERROR_DOC),
    ]
)


def _open_interest(exchange: PublicExchange, symbol: str) -> Row:
    oi = exchange.call("fetch_open_interest", symbol)
    return {
        "ts": us(int_or_none(oi.get("timestamp"))),
        "open_interest_amount": num(oi.get("openInterestAmount")),
        "open_interest_value": num(oi.get("openInterestValue")),
    }


@init_single_worker
class OpenInterest(TableFunctionGenerator[FanoutArgs, FanoutCursor]):
    """Current open interest of a derivatives market, across one or more exchanges."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OPEN_INTEREST_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "open_interest"
        description = "Current open interest of a perpetual or futures market, across one or more exchanges"
        tags = {
            **object_tags(
                title="Open Interest",
                category="derivatives",
                keywords="open interest, oi, outstanding contracts, perpetual swap, futures, positioning",
                doc_llm=(
                    "Current open interest of a derivatives market: the amount (contracts or base "
                    "currency, as the exchange reports it) and, where published, its value in the quote "
                    "currency. First argument: one exchange id or a list; second: the unified symbol "
                    "('BTC/USDT:USDT'). Exchanges report different units and some publish only one of the "
                    "two columns, so compare `open_interest_value` across venues. Unsupported exchanges "
                    "return a row with `error` set."
                ),
                doc_md=(
                    "## open_interest(exchange | [exchange, ...], symbol)\n\n"
                    "Open interest per exchange, in contracts/base and in quote value.\n\n" + TERMS_NOTE
                ),
            ),
            "vgi.result_columns_schema": result_columns(OPEN_INTEREST_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT exchange, open_interest_value FROM ccxt.main.open_interest("
                    "['binance', 'okx'], 'BTC/USDT:USDT') WHERE error IS NULL "
                    "ORDER BY open_interest_value DESC"
                ),
                description="BTC perpetual open interest by venue, largest first",
            ),
            FunctionExample(
                sql="SELECT open_interest_amount FROM ccxt.main.open_interest('okx', 'ETH/USDT:USDT')",
                description="ETH perpetual open interest on OKX",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FanoutArgs]) -> BindResponse:
        """Validate every exchange id up front."""
        return _fanout_bind(params, OPEN_INTEREST_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FanoutArgs]) -> FanoutCursor:
        """Queue the exchanges to visit."""
        return _fanout_state(params)

    @classmethod
    def process(cls, params: ProcessParams[FanoutArgs], state: FanoutCursor, out: OutputCollector) -> None:
        """Fetch the next exchange's open interest."""
        _fanout_tick(params, state, out, "fetchOpenInterest", _open_interest)


# =============================================================================
# order_book
# =============================================================================

ORDER_BOOK_SCHEMA = pa.schema(
    [
        column("exchange", pa.string(), "CCXT exchange id.", nullable=False),
        column("symbol", pa.string(), "CCXT unified market symbol.", nullable=False),
        column("ts", TIMESTAMP, "Snapshot time reported by the exchange, UTC (NULL if it does not say)."),
        column("side", pa.string(), "'bid' (buy orders) or 'ask' (sell orders).", nullable=False),
        column("level", pa.int64(), "Depth level, 1 = best price on that side.", nullable=False),
        column("price", pa.float64(), "Price level, in the quote currency.", nullable=False),
        column("amount", pa.float64(), "Quantity resting at this price, in the base currency."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class OrderBookArgs:
    """Arguments for ``order_book(exchange, symbol, depth := 20)``."""

    exchange: Annotated[str, Arg(0, doc="CCXT exchange id, e.g. 'kraken'.")]
    symbol: Annotated[str, Arg(1, doc="CCXT unified symbol, e.g. 'BTC/USD'.")]
    depth: Annotated[
        int,
        Arg("depth", default=20, doc="Price levels per side (default 20); exchanges may return fewer.", ge=1, le=1000),
    ] = 20


@dataclass(kw_only=True)
class OnceCursor(ArrowSerializableDataclass):
    """A single-batch snapshot: set once the batch is out."""

    done: bool = False


@init_single_worker
class OrderBook(TableFunctionGenerator[OrderBookArgs, OnceCursor]):
    """A depth snapshot of one market's order book."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ORDER_BOOK_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "order_book"
        description = "Order-book depth snapshot for one market: bids and asks by level"
        tags = {
            **object_tags(
                title="Order Book Depth",
                category="snapshots",
                keywords="order book, depth, level 2, l2, bids, asks, liquidity, spread, market depth",
                doc_llm=(
                    "A snapshot of one market's level-2 order book: one row per price level and side. "
                    "Positional: exchange id and unified symbol; named `depth` (levels per side, default "
                    "20, up to 1000; exchanges may return fewer). `side` is 'bid' or 'ask', `level` 1 is "
                    "the best price, `amount` is base currency. Use it for spread, mid price and "
                    "liquidity within a price band."
                ),
                doc_md=(
                    "## order_book(exchange, symbol, depth := 20)\n\n"
                    "Bids and asks by level, best first.\n\n" + TERMS_NOTE
                ),
            ),
            "vgi.result_columns_schema": result_columns(ORDER_BOOK_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT side, sum(amount) AS btc, sum(price * amount) AS usd "
                    "FROM ccxt.main.order_book('kraken', 'BTC/USD', depth := 50) GROUP BY side ORDER BY side"
                ),
                description="Liquidity in the top 50 levels on each side of Kraken's BTC/USD book",
            ),
            FunctionExample(
                sql=(
                    "SELECT max(price) FILTER (side = 'bid') AS best_bid, min(price) FILTER (side = 'ask') "
                    "AS best_ask FROM ccxt.main.order_book('coinbase', 'ETH/USD', depth := 1)"
                ),
                description="Best bid and ask for ETH/USD on Coinbase",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[OrderBookArgs]) -> BindResponse:
        """Validate the exchange and its order-book support."""
        get_exchange(params.args.exchange).require("fetchOrderBook")
        return BindResponse(output_schema=ORDER_BOOK_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[OrderBookArgs]) -> OnceCursor:
        """Nothing emitted yet."""
        return OnceCursor()

    @classmethod
    def process(cls, params: ProcessParams[OrderBookArgs], state: OnceCursor, out: OutputCollector) -> None:
        """Fetch the book once and emit both sides as one batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        args = params.args
        exchange = get_exchange(args.exchange)
        book = exchange.call("fetch_order_book", args.symbol, args.depth)
        ts = us(int_or_none(book.get("timestamp")))
        rows: list[tuple[str, int, float, float | None]] = []
        for side, key in (("bid", "bids"), ("ask", "asks")):
            for level, entry in enumerate((book.get(key) or [])[: args.depth], start=1):
                price = num(entry[0])
                if price is not None:
                    rows.append((side, level, price, num(entry[1]) if len(entry) > 1 else None))
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "exchange": [exchange.id] * len(rows),
                    "symbol": [args.symbol] * len(rows),
                    "ts": [ts] * len(rows),
                    "side": [r[0] for r in rows],
                    "level": [r[1] for r in rows],
                    "price": [r[2] for r in rows],
                    "amount": [r[3] for r in rows],
                },
                schema=params.output_schema,
            )
        )
