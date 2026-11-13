"""History table functions: ``ohlcv``, ``trades`` and ``funding_rates``.

Each one backfills a ``[since, until)`` window by calling the exchange
repeatedly, one page per ``process`` tick, and streams each page to DuckDB as
one Arrow batch. The position lives in a serializable cursor (the ``*Cursor``
dataclasses), never in process memory: over HTTP each tick can be served by a
different worker process, and the cursor travels with the request.

Termination, in order: the window's end is reached, ``max_rows`` rows are out,
or a page brings nothing new. The last rule also stops exchanges that ignore
``since`` (they keep returning the same rows, which the cursor filters out).
Without ``since`` a function returns the single most recent page.
"""

from __future__ import annotations

import datetime as dt
import logging
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
from vgi_rpc.log import Level

from .client import PublicExchange, UntilRequiredError, get_exchange, now_ms, parse_timeframe_ms
from .common import (
    HOUR_MS,
    TIME_TYPES,
    TIMESTAMP,
    annualize,
    interval_hours_from_gap,
    interval_hours_from_label,
    num,
    time_arg_ms,
    us,
)
from .meta import TERMS_NOTE, column, object_tags, result_columns

LOG = logging.getLogger(__name__)

DEFAULT_MAX_ROWS = 100_000
MAX_MAX_ROWS = 10_000_000
#: Fallback page sizes when CCXT documents no per-exchange limit.
DEFAULT_PAGE = {"fetchOHLCV": 500, "fetchTrades": 500, "fetchFundingRateHistory": 200}
#: A trades page made only of already-seen fills is refetched a millisecond later at most this often per tick.
MAX_EMPTY_REFETCH = 5
#: Time windows for exchanges whose trades endpoint needs an explicit ``until`` (Coinbase).
WINDOW_START_MS = 10 * 60_000
WINDOW_MIN_MS = 1_000
WINDOW_MAX_MS = 6 * 3_600_000
MAX_WINDOWS_PER_TICK = 50

_MAX_ROWS_DOC = "Stop after this many rows (default 100,000)."

_EXCHANGE: Arg[str] = Arg(0, doc="CCXT exchange id such as 'kraken'; ccxt.main.exchanges lists every id.")
_SYMBOL: Arg[str] = Arg(
    1, doc="CCXT unified market symbol such as 'BTC/USD' (spot) or 'BTC/USDT:USDT' (USDT-settled perpetual)."
)


def _window(since: Any, until: Any, since_name: str, until_name: str) -> tuple[int | None, int]:
    """Validate and convert a window's bounds; a missing end means now.

    An explicit window that ends before it starts is an error. A start after a
    defaulted end (a start in the future) is just an empty window: nothing has
    happened there yet.
    """
    start = time_arg_ms(since, since_name)
    end = time_arg_ms(until, until_name)
    if end is None:
        end = now_ms()
        if start is not None and start >= end:
            return start, start
    if start is not None and start >= end:
        raise ValueError(f"{since_name} must be earlier than {until_name}")
    return start, end


def _is_empty(start: int | None, end: int) -> bool:
    return start is not None and start >= end


def _iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).strftime("%Y-%m-%d %H:%M UTC")


def _warn_if_history_starts_late(
    out: OutputCollector, exchange_id: str, what: str, since: int, first: int | None, slack_ms: int
) -> None:
    """Tell the caller when the exchange's first page starts well after ``since``.

    Many exchanges keep only recent history (Kraken: the latest 720 candles; OKX: about
    three months of funding) or ignore ``since`` entirely, and return later rows without
    an error. The query is still correct for what the exchange has; this says why it is short.
    """
    if first is not None and first > since + slack_ms:
        message = (
            f"{exchange_id} returned {what} starting {_iso(first)}, after the requested start "
            f"{_iso(since)}; the exchange probably keeps only recent history or ignores the start"
        )
        LOG.warning("%s", message)
        out.client_log(Level.WARN, message)


def _page_size(exchange: PublicExchange, capability: str, remaining: int) -> int:
    return max(1, min(exchange.page_limit(capability, DEFAULT_PAGE[capability]), remaining))


# =============================================================================
# ohlcv
# =============================================================================

OHLCV_SCHEMA = pa.schema(
    [
        column("exchange", pa.string(), "CCXT exchange id the candle came from.", nullable=False),
        column("symbol", pa.string(), "CCXT unified market symbol, e.g. 'BTC/USD'.", nullable=False),
        column("timeframe", pa.string(), "Candle length, e.g. '1h' or '1d'.", nullable=False),
        column("ts", TIMESTAMP, "Candle open time, UTC.", nullable=False),
        column("open", pa.float64(), "First trade price in the candle, in the quote currency."),
        column("high", pa.float64(), "Highest trade price in the candle, in the quote currency."),
        column("low", pa.float64(), "Lowest trade price in the candle, in the quote currency."),
        column("close", pa.float64(), "Last trade price in the candle, in the quote currency."),
        column("volume", pa.float64(), "Traded volume in the candle, in the base currency."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class OhlcvArgs:
    """Arguments for ``ohlcv(exchange, symbol, timeframe, since :=, until :=, max_rows :=)``."""

    exchange: Annotated[str, _EXCHANGE]
    symbol: Annotated[str, _SYMBOL]
    timeframe: Annotated[
        str, Arg(2, doc="Candle length in CCXT notation such as '1h' or '1d', checked against the exchange's own list.")
    ]
    since: Annotated[
        AnyArrowValue | None,
        Arg(
            "since",
            default=None,
            type_bound=TIME_TYPES,
            doc="Start of the window in UTC, inclusive; dates and ISO-8601 strings work too. Omit for the latest page.",
        ),
    ] = None
    until: Annotated[
        AnyArrowValue | None,
        Arg(
            "until",
            default=None,
            type_bound=TIME_TYPES,
            doc="End of the window in UTC, exclusive. Defaults to now.",
        ),
    ] = None
    max_rows: Annotated[int, Arg("max_rows", default=DEFAULT_MAX_ROWS, doc=_MAX_ROWS_DOC, ge=1, le=MAX_MAX_ROWS)] = (
        DEFAULT_MAX_ROWS
    )


@dataclass(kw_only=True)
class OhlcvCursor(ArrowSerializableDataclass):
    """Externalized since-cursor for ``ohlcv``: survives a change of worker process between pages."""

    next_since_ms: int = -1
    until_ms: int = 0
    emitted: int = 0
    pages: int = 0
    done: bool = False


@init_single_worker
class Ohlcv(TableFunctionGenerator[OhlcvArgs, OhlcvCursor]):
    """Candles for one market, backfilled over a window one exchange page at a time."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OHLCV_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "ohlcv"
        description = "OHLCV candles for one market on one exchange, backfilled over a time window"
        tags = {
            **object_tags(
                title="OHLCV Candles",
                category="history",
                keywords="ohlcv, candles, candlesticks, klines, bars, price history, backfill, open high low close",
                doc_llm=(
                    "Open/high/low/close/volume candles for one market. Positional arguments: exchange id "
                    "(e.g. 'coinbase'), unified symbol (e.g. 'BTC/USD') and timeframe (e.g. '1h', '1d'). "
                    "Named arguments: `since := TIMESTAMP '2025-01-01'` (inclusive start, UTC), "
                    "`until := TIMESTAMP '2025-02-01'` (exclusive end, default now) and `max_rows` "
                    "(default 100,000). With `since` the worker pages forward through the window with a "
                    "cursor until it reaches `until`; without `since` it returns only the latest page of "
                    "candles (the exchange's default count). `ts` is the candle's open time in UTC; prices "
                    "are in the quote currency and volume in the base currency. Some exchanges keep only "
                    "recent history (Kraken serves the latest 720 candles), so an old `since` can return "
                    "fewer rows than the window implies; the worker then logs a warning naming the first "
                    "candle the exchange returned. An unsupported timeframe is rejected at bind with "
                    "the list the exchange offers."
                ),
                doc_md=(
                    "## ohlcv(exchange, symbol, timeframe, since := …, until := …, max_rows := 100000)\n\n"
                    "Candles over `[since, until)`, paged with a since-cursor; one batch per exchange page.\n\n"
                    + TERMS_NOTE
                ),
            ),
            "vgi.result_columns_schema": result_columns(OHLCV_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT ts, close, volume FROM ccxt.main.ohlcv('coinbase', 'BTC/USD', '1d', "
                    "since := TIMESTAMP '2025-01-01', until := TIMESTAMP '2025-02-01') ORDER BY ts"
                ),
                description="Daily BTC/USD candles on Coinbase for January 2025",
            ),
            FunctionExample(
                sql=(
                    "SELECT date_trunc('day', ts) AS day, max(high) - min(low) AS range_usdt "
                    "FROM ccxt.main.ohlcv('okx', 'ETH/USDT', '1h', "
                    "since := TIMESTAMP '2025-01-01', until := TIMESTAMP '2025-01-08') GROUP BY day ORDER BY day"
                ),
                description="Daily high-low range of ETH/USDT on OKX in the first week of 2025, from hourly candles",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[OhlcvArgs]) -> BindResponse:
        """Validate the exchange, its OHLCV support, the timeframe and the window."""
        args = params.args
        exchange = get_exchange(args.exchange)
        exchange.require("fetchOHLCV")
        offered = exchange.timeframes
        if offered and args.timeframe not in offered:
            raise ValueError(
                f"exchange '{exchange.id}' does not offer timeframe '{args.timeframe}'; it offers {', '.join(offered)}"
            )
        parse_timeframe_ms(args.timeframe)
        _window(args.since, args.until, "since", "until")
        return BindResponse(output_schema=OHLCV_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[OhlcvArgs]) -> OhlcvCursor:
        """Fix the window once, so a default ``until`` of now cannot move between pages."""
        since, until = _window(params.args.since, params.args.until, "since", "until")
        return OhlcvCursor(next_since_ms=-1 if since is None else since, until_ms=until, done=_is_empty(since, until))

    @classmethod
    def process(cls, params: ProcessParams[OhlcvArgs], state: OhlcvCursor, out: OutputCollector) -> None:
        """Fetch one page from the cursor, emit it, and advance the cursor past its last candle."""
        if state.done:
            out.finish()
            return
        args = params.args
        exchange = get_exchange(args.exchange)
        remaining = args.max_rows - state.emitted
        since = None if state.next_since_ms < 0 else state.next_since_ms
        page = exchange.call(
            "fetch_ohlcv", args.symbol, args.timeframe, since, _page_size(exchange, "fetchOHLCV", remaining)
        )
        if state.pages == 0 and since is not None:
            first = min((int(c[0]) for c in page or [] if c and c[0] is not None), default=None)
            _warn_if_history_starts_late(
                out, exchange.id, "candles", since, first, 2 * parse_timeframe_ms(args.timeframe)
            )
        state.pages += 1
        by_ts: dict[int, list[Any]] = {}
        for candle in page or []:
            ts = candle[0] if candle else None
            if ts is None or ts >= state.until_ms or (since is not None and ts < since):
                continue
            by_ts[int(ts)] = candle
        candles = [by_ts[ts] for ts in sorted(by_ts)]
        candles = candles[-remaining:] if since is None else candles[:remaining]
        if not candles:
            out.finish()
            return
        state.emitted += len(candles)
        last = int(candles[-1][0])
        state.next_since_ms = last + parse_timeframe_ms(args.timeframe)
        state.done = since is None or state.next_since_ms >= state.until_ms or state.emitted >= args.max_rows
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "exchange": [exchange.id] * len(candles),
                    "symbol": [args.symbol] * len(candles),
                    "timeframe": [args.timeframe] * len(candles),
                    "ts": [us(int(c[0])) for c in candles],
                    "open": [num(c[1]) for c in candles],
                    "high": [num(c[2]) for c in candles],
                    "low": [num(c[3]) for c in candles],
                    "close": [num(c[4]) for c in candles],
                    "volume": [num(c[5]) if len(c) > 5 else None for c in candles],
                },
                schema=params.output_schema,
            )
        )


# =============================================================================
# trades
# =============================================================================

TRADES_SCHEMA = pa.schema(
    [
        column("exchange", pa.string(), "CCXT exchange id the trade came from.", nullable=False),
        column("symbol", pa.string(), "CCXT unified market symbol, e.g. 'BTC/USD'.", nullable=False),
        column("trade_id", pa.string(), "The exchange's identifier for the trade, when it publishes one."),
        column("ts", TIMESTAMP, "Execution time, UTC.", nullable=False),
        column("side", pa.string(), "Taker side: 'buy' or 'sell'."),
        column("price", pa.float64(), "Execution price, in the quote currency."),
        column("amount", pa.float64(), "Executed quantity, in the base currency (contracts for some derivatives)."),
        column("cost", pa.float64(), "price times amount, in the quote currency."),
        column("taker_or_maker", pa.string(), "'taker' or 'maker' when the exchange reports it."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class TradesArgs:
    """Arguments for ``trades(exchange, symbol, since :=, until :=, max_rows :=)``."""

    exchange: Annotated[str, _EXCHANGE]
    symbol: Annotated[str, _SYMBOL]
    since: Annotated[
        AnyArrowValue | None,
        Arg(
            "since",
            default=None,
            type_bound=TIME_TYPES,
            doc="Start of the window in UTC, inclusive. Omit for the most recent page of trades only.",
        ),
    ] = None
    until: Annotated[
        AnyArrowValue | None,
        Arg("until", default=None, type_bound=TIME_TYPES, doc="End of the window in UTC, exclusive. Defaults to now."),
    ] = None
    max_rows: Annotated[int, Arg("max_rows", default=DEFAULT_MAX_ROWS, doc=_MAX_ROWS_DOC, ge=1, le=MAX_MAX_ROWS)] = (
        DEFAULT_MAX_ROWS
    )


@dataclass(kw_only=True)
class TradesCursor(ArrowSerializableDataclass):
    """Externalized cursor for ``trades``.

    ``next_since_ms`` is inclusive, because several fills can share the last
    millisecond of a page; ``boundary_keys`` are the fills already emitted at
    that millisecond, so the next page skips them instead of repeating them.
    Exchanges that insist on an explicit window end (Coinbase) are walked in
    ``window_ms`` windows instead; see ``_window_page``.
    """

    next_since_ms: int = -1
    until_ms: int = 0
    boundary_keys: list[str] = field(default_factory=list)
    #: 0 while the exchange pages by ``since`` alone; otherwise the current window length.
    window_ms: int = 0
    emitted: int = 0
    done: bool = False


def _trade_key(trade: dict[str, Any]) -> str:
    trade_id = trade.get("id")
    if trade_id is not None:
        return str(trade_id)
    return f"{trade.get('timestamp')}:{trade.get('price')}:{trade.get('amount')}:{trade.get('side')}"


@init_single_worker
class Trades(TableFunctionGenerator[TradesArgs, TradesCursor]):
    """Public trades for one market, backfilled over a window without gaps or repeats at page edges."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TRADES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "trades"
        description = "Public trades (time and sales) for one market on one exchange over a time window"
        tags = {
            **object_tags(
                title="Public Trades",
                category="history",
                keywords="trades, fills, time and sales, tape, executions, tick data, prints",
                doc_llm=(
                    "Public executed trades for one market. Positional: exchange id and unified symbol. "
                    "Named: `since` (inclusive start, UTC), `until` (exclusive end, default now) and "
                    "`max_rows` (default 100,000). With `since` the worker pages forward with a cursor that "
                    "keeps fills sharing a millisecond intact across page boundaries (exchanges that need "
                    "an explicit window end, such as Coinbase, are walked in self-sizing time windows); "
                    "without `since` it returns the exchange's most recent page. Many exchanges only serve "
                    "recent trades publicly (hours to days), so a deep `since` may start later than requested. "
                    "`side` "
                    "is the taker side; `amount` is base currency; `cost` is price times amount."
                ),
                doc_md=(
                    "## trades(exchange, symbol, since := …, until := …, max_rows := 100000)\n\n"
                    "Executed trades over `[since, until)`, paged with a since-cursor that de-duplicates "
                    "fills at page edges.\n\n" + TERMS_NOTE
                ),
            ),
            "vgi.result_columns_schema": result_columns(TRADES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT side, count(*) AS trades, sum(amount) AS btc "
                    "FROM ccxt.main.trades('kraken', 'BTC/USD') GROUP BY side ORDER BY side"
                ),
                description="Buy versus sell volume in Kraken's most recent BTC/USD trades",
            ),
            FunctionExample(
                sql=(
                    "SELECT sum(price * amount) / sum(amount) AS vwap, min(ts) AS first_trade "
                    "FROM ccxt.main.trades('coinbase', 'ETH/USD')"
                ),
                description="Volume-weighted average price over Coinbase's most recent ETH/USD trades",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TradesArgs]) -> BindResponse:
        """Validate the exchange, its trades support and the window."""
        get_exchange(params.args.exchange).require("fetchTrades")
        _window(params.args.since, params.args.until, "since", "until")
        return BindResponse(output_schema=TRADES_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[TradesArgs]) -> TradesCursor:
        """Fix the window once."""
        since, until = _window(params.args.since, params.args.until, "since", "until")
        return TradesCursor(next_since_ms=-1 if since is None else since, until_ms=until, done=_is_empty(since, until))

    @classmethod
    def process(cls, params: ProcessParams[TradesArgs], state: TradesCursor, out: OutputCollector) -> None:
        """Fetch the next page (or window) of fills and emit the ones not yet emitted."""
        if state.done:
            out.finish()
            return
        args = params.args
        exchange = get_exchange(args.exchange)
        remaining = args.max_rows - state.emitted
        size = _page_size(exchange, "fetchTrades", remaining)
        fresh: list[dict[str, Any]] | None = None
        if state.window_ms == 0:
            try:
                fresh = _cursor_page(exchange, args.symbol, state, size, remaining)
            except UntilRequiredError:
                LOG.info("%s fetchTrades needs an explicit until; paging by time window", exchange.id)
                state.window_ms = WINDOW_START_MS
        if fresh is None:
            fresh = _window_page(exchange, args.symbol, state, size, remaining)
        if not fresh and (state.done or state.window_ms == 0):
            out.finish()
            return
        state.emitted += len(fresh)
        state.done = state.done or state.emitted >= args.max_rows
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "exchange": [exchange.id] * len(fresh),
                    "symbol": [args.symbol] * len(fresh),
                    "trade_id": [None if t.get("id") is None else str(t["id"]) for t in fresh],
                    "ts": [us(int(t["timestamp"])) for t in fresh],
                    "side": [t.get("side") for t in fresh],
                    "price": [num(t.get("price")) for t in fresh],
                    "amount": [num(t.get("amount")) for t in fresh],
                    "cost": [num(t.get("cost")) for t in fresh],
                    "taker_or_maker": [t.get("takerOrMaker") for t in fresh],
                },
                schema=params.output_schema,
            )
        )


def _select_trades(raw: list[dict[str, Any]], since: int | None, end: int, skip: set[str]) -> list[dict[str, Any]]:
    """Return fills in ``[since, end)``, oldest first, minus repeats and the ``skip`` keys at ``since``."""
    fresh: list[dict[str, Any]] = []
    keys: set[str] = set()
    for trade in sorted(raw, key=lambda t: (int(t["timestamp"]), _trade_key(t))):
        ts = int(trade["timestamp"])
        key = _trade_key(trade)
        if ts >= end or (since is not None and (ts < since or (ts == since and key in skip))) or key in keys:
            continue
        keys.add(key)
        fresh.append(trade)
    return fresh


def _fetch_trades(
    exchange: PublicExchange, symbol: str, since: int | None, size: int, until_ms: int | None = None
) -> list[dict[str, Any]]:
    page = exchange.call("fetch_trades", symbol, since, size, until_ms=until_ms) or []
    return [t for t in page if t.get("timestamp")]


def _cursor_page(
    exchange: PublicExchange, symbol: str, state: TradesCursor, size: int, remaining: int
) -> list[dict[str, Any]]:
    """Page forward by ``since`` alone, as most exchanges allow."""
    fresh: list[dict[str, Any]] = []
    since: int | None = None
    for _ in range(MAX_EMPTY_REFETCH):
        since = None if state.next_since_ms < 0 else state.next_since_ms
        raw = _fetch_trades(exchange, symbol, since, size)
        fresh = _select_trades(raw, since, state.until_ms, set(state.boundary_keys))
        # A full page of nothing but already-emitted fills at one millisecond: step past it.
        if not fresh and since is not None and len(raw) >= size and all(int(t["timestamp"]) == since for t in raw):
            LOG.warning("%s: more than %d trades at %d ms; skipping ahead 1 ms", exchange.id, size, since)
            state.next_since_ms, state.boundary_keys = since + 1, []
            continue
        break
    fresh = fresh[-remaining:] if since is None else fresh[:remaining]
    if not fresh:
        state.done = True
        return fresh
    last = int(fresh[-1]["timestamp"])
    keys_at_last = [_trade_key(t) for t in fresh if int(t["timestamp"]) == last]
    state.boundary_keys = (state.boundary_keys if last == state.next_since_ms else []) + keys_at_last
    state.next_since_ms = last
    state.done = since is None
    return fresh


def _window_page(
    exchange: PublicExchange, symbol: str, state: TradesCursor, size: int, remaining: int
) -> list[dict[str, Any]]:
    """Walk ``[since, until)`` in explicit time windows, for exchanges that require ``until``.

    A window that comes back full (as many fills as the page size) may have been
    truncated, so it is halved and fetched again; sparse windows double, up to
    ``WINDOW_MAX_MS``. Fills at or after a window's end are left for the next
    window, so nothing repeats.
    """
    for _ in range(MAX_WINDOWS_PER_TICK):
        since = state.next_since_ms
        if since >= state.until_ms:
            state.done = True
            return []
        end = min(since + state.window_ms, state.until_ms)
        # Exchanges round `until` to their own resolution (Coinbase: whole seconds) and differ on
        # whether it is inclusive, so ask through `end` and drop anything at or after it.
        raw = _fetch_trades(exchange, symbol, since, size, until_ms=end)
        if len(raw) >= size and state.window_ms > WINDOW_MIN_MS:
            state.window_ms = max(WINDOW_MIN_MS, state.window_ms // 2 // 1000 * 1000)
            continue
        if len(raw) >= size:
            LOG.warning(
                "%s: %d+ trades within %d ms at %d; the page may be truncated", exchange.id, size, end - since, since
            )
        fresh = _select_trades(raw, since, end, set())
        state.next_since_ms, state.boundary_keys = end, []
        state.done = end >= state.until_ms
        if len(raw) < size // 4:
            state.window_ms = min(WINDOW_MAX_MS, state.window_ms * 2)
        if fresh or state.done:
            return fresh[:remaining]
    return []


# =============================================================================
# funding_rates
# =============================================================================

#: Exchanges whose published funding sign is the reverse of "positive = longs pay shorts".
#: CCXT passes each exchange's published rate through unchanged. Binance, Bybit and OKX
#: document positive = longs pay shorts, and no exchange with the reverse sign is known, so
#: this is empty. Add an id here, with a test, if one is found.
FLIPPED_SIGN_EXCHANGES: frozenset[str] = frozenset()

FUNDING_HISTORY_SCHEMA = pa.schema(
    [
        column("exchange", pa.string(), "CCXT exchange id.", nullable=False),
        column("symbol", pa.string(), "CCXT unified perpetual symbol, e.g. 'BTC/USDT:USDT'.", nullable=False),
        column("ts", TIMESTAMP, "Funding settlement time, UTC.", nullable=False),
        column(
            "funding_rate",
            pa.float64(),
            "Rate charged for the interval ending at ts, as a decimal (0.0001 = 0.01%). "
            "Positive means longs pay shorts; negative means shorts pay longs.",
        ),
        column(
            "interval_hours",
            pa.float64(),
            "Length of the funding interval in hours (1, 4, 8, ...), inferred from the spacing of settlements.",
        ),
        column(
            "funding_rate_annualized",
            pa.float64(),
            "funding_rate times the intervals in a 365-day year (simple, not compounded).",
        ),
    ]
)


def signed_rate(exchange_id: str, rate: Any) -> float | None:
    """Return a funding rate in the worker's convention: positive = longs pay shorts."""
    value = num(rate)
    if value is None:
        return None
    return -value if exchange_id in FLIPPED_SIGN_EXCHANGES else value


@dataclass(slots=True, frozen=True, kw_only=True)
class FundingRatesArgs:
    """Arguments for ``funding_rates(exchange, symbol, start_date :=, end_date :=, max_rows :=)``."""

    exchange: Annotated[str, _EXCHANGE]
    symbol: Annotated[str, Arg(1, doc="CCXT unified perpetual symbol such as 'BTC/USDT:USDT'.")]
    start_date: Annotated[
        AnyArrowValue | None,
        Arg(
            "start_date",
            default=None,
            type_bound=TIME_TYPES,
            doc="First settlement to include, UTC, inclusive. Omit for the most recent page only.",
        ),
    ] = None
    end_date: Annotated[
        AnyArrowValue | None,
        Arg(
            "end_date",
            default=None,
            type_bound=TIME_TYPES,
            doc="Include settlements before this time, UTC, exclusive. Defaults to now.",
        ),
    ] = None
    max_rows: Annotated[int, Arg("max_rows", default=DEFAULT_MAX_ROWS, doc=_MAX_ROWS_DOC, ge=1, le=MAX_MAX_ROWS)] = (
        DEFAULT_MAX_ROWS
    )


@dataclass(kw_only=True)
class FundingCursor(ArrowSerializableDataclass):
    """Externalized cursor for ``funding_rates``, including the last settlement for interval inference."""

    next_since_ms: int = -1
    until_ms: int = 0
    prev_ts: int = -1
    last_interval_hours: float = 0.0
    emitted: int = 0
    done: bool = False


@init_single_worker
class FundingRates(TableFunctionGenerator[FundingRatesArgs, FundingCursor]):
    """Settled funding-rate history for one perpetual swap."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FUNDING_HISTORY_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "funding_rates"
        description = "Historical funding rates of a perpetual swap, per interval and annualized"
        tags = {
            **object_tags(
                title="Funding Rate History",
                category="derivatives",
                keywords="funding rate, funding history, perpetual swap, perps, basis, carry, derivatives",
                doc_llm=(
                    "Settled funding rates of one perpetual swap over a window. Positional: exchange id "
                    "(e.g. 'binance', 'okx', 'bybit') and the unified perpetual symbol "
                    "('BTC/USDT:USDT'). Named: `start_date` (inclusive, UTC), `end_date` (exclusive, "
                    "default now), `max_rows`. `funding_rate` is a decimal per funding interval with the "
                    "convention positive = longs pay shorts; `interval_hours` is inferred from the spacing "
                    "of settlements (exchanges use 1, 4 or 8 hours and some switch); "
                    "`funding_rate_annualized` multiplies by intervals per 365-day year. Without "
                    "`start_date` only the latest page is returned. Retention differs by venue: Hyperliquid "
                    "serves long history, OKX about three months, KuCoin about one, Gate 180 days, and "
                    "Bitget only its latest page; when an exchange starts later than `start_date` the "
                    "worker logs a warning and returns what the exchange has. Fails with a clear error on "
                    "exchanges without fetchFundingRateHistory (spot-only venues such as Kraken or Coinbase)."
                ),
                doc_md=(
                    "## funding_rates(exchange, symbol, start_date := …, end_date := …, max_rows := 100000)\n\n"
                    "Settled funding of a perpetual, decimal per interval (positive = longs pay shorts) "
                    "plus a simple annualized rate.\n\n" + TERMS_NOTE
                ),
            ),
            "vgi.result_columns_schema": result_columns(FUNDING_HISTORY_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT avg(funding_rate_annualized) AS avg_apr "
                    "FROM ccxt.main.funding_rates('okx', 'BTC/USDT:USDT', "
                    "start_date := current_date - INTERVAL 30 DAY)"
                ),
                description="Average annualized BTC perpetual funding on OKX over the last 30 days",
            ),
            FunctionExample(
                sql=(
                    "SELECT date_trunc('day', ts) AS day, sum(funding_rate) AS daily_funding "
                    "FROM ccxt.main.funding_rates('binance', 'ETH/USDT:USDT', "
                    "start_date := DATE '2025-01-01', end_date := DATE '2025-01-15') GROUP BY day ORDER BY day"
                ),
                description="Daily total ETH perpetual funding on Binance over the first two weeks of 2025",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FundingRatesArgs]) -> BindResponse:
        """Validate the exchange, its funding-history support and the window."""
        get_exchange(params.args.exchange).require("fetchFundingRateHistory")
        _window(params.args.start_date, params.args.end_date, "start_date", "end_date")
        return BindResponse(output_schema=FUNDING_HISTORY_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FundingRatesArgs]) -> FundingCursor:
        """Fix the window once."""
        start, end = _window(params.args.start_date, params.args.end_date, "start_date", "end_date")
        return FundingCursor(next_since_ms=-1 if start is None else start, until_ms=end, done=_is_empty(start, end))

    @classmethod
    def _declared_interval(cls, exchange: PublicExchange, symbol: str) -> float | None:
        """Return the declared interval from fetch_funding_rate, for a lone settlement."""
        if exchange.status("fetchFundingRate") not in ("native", "emulated"):
            return None
        try:
            current = exchange.call("fetch_funding_rate", symbol)
        except Exception as err:  # best effort: only used to label a single row
            LOG.info("%s: no declared funding interval for %s: %s", exchange.id, symbol, err)
            return None
        return interval_hours_from_label(current.get("interval"))

    @classmethod
    def process(cls, params: ProcessParams[FundingRatesArgs], state: FundingCursor, out: OutputCollector) -> None:
        """Fetch one page of settlements, label each with its interval, emit, advance."""
        if state.done:
            out.finish()
            return
        args = params.args
        exchange = get_exchange(args.exchange)
        remaining = args.max_rows - state.emitted
        since = None if state.next_since_ms < 0 else state.next_since_ms
        size = _page_size(exchange, "fetchFundingRateHistory", remaining)
        page = exchange.call("fetch_funding_rate_history", args.symbol, since, size) or []
        if state.prev_ts < 0 and since is not None:
            first = min((int(r["timestamp"]) for r in page if r.get("timestamp") is not None), default=None)
            _warn_if_history_starts_late(out, exchange.id, "funding settlements", since, first, 25 * HOUR_MS)
        by_ts: dict[int, dict[str, Any]] = {}
        for row in page:
            ts = row.get("timestamp")
            if ts is None or ts >= state.until_ms or (since is not None and ts < since) or ts <= state.prev_ts:
                continue
            by_ts[int(ts)] = row
        rows = [by_ts[ts] for ts in sorted(by_ts)]
        rows = rows[-remaining:] if since is None else rows[:remaining]
        if not rows:
            out.finish()
            return
        times = [int(r["timestamp"]) for r in rows]
        intervals: list[float | None] = []
        last_known = state.last_interval_hours or None
        for i, ts in enumerate(times):
            prev = times[i - 1] if i else (state.prev_ts if state.prev_ts >= 0 else None)
            hours = interval_hours_from_gap(None if prev is None else ts - prev)
            if hours is None and prev is None and len(times) > 1:
                hours = interval_hours_from_gap(times[1] - ts)
            if hours is None and prev is None and len(times) == 1:
                hours = cls._declared_interval(exchange, args.symbol)
            hours = hours or last_known
            intervals.append(hours)
            last_known = hours
        rates = [signed_rate(exchange.id, r.get("fundingRate")) for r in rows]
        state.prev_ts = times[-1]
        state.last_interval_hours = last_known or 0.0
        state.next_since_ms = times[-1] + 1
        state.emitted += len(rows)
        state.done = since is None or state.next_since_ms >= state.until_ms or state.emitted >= args.max_rows
        out.emit(
            pa.RecordBatch.from_pydict(
                {
                    "exchange": [exchange.id] * len(rows),
                    "symbol": [args.symbol] * len(rows),
                    "ts": [us(t) for t in times],
                    "funding_rate": rates,
                    "interval_hours": intervals,
                    "funding_rate_annualized": [annualize(r, h) for r, h in zip(rates, intervals, strict=True)],
                },
                schema=params.output_schema,
            )
        )


__all__ = ["FundingRates", "Ohlcv", "Trades"]
