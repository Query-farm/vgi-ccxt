"""Discovery metadata for the catalog, the schema and every function.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these tags.
Every object carries ``object_tags(...)``: a title, a category from
``SCHEMA_CATEGORIES``, search keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "ccxt"

SOURCE_URL = "https://github.com/Query-farm/vgi-ccxt"

#: Appended to every function's human docs: the data is the exchanges', not ours.
TERMS_NOTE = (
    "Every call is a live request from your machine to the exchange's public API. Nothing is "
    "cached or redistributed by this worker; you are the API caller, and each exchange's terms "
    "of use and geographic restrictions apply to you (for example, Binance.com refuses US IP "
    "addresses)."
)

SCHEMA_CATEGORIES = [
    {
        "name": "history",
        "title": "Historical Market Data",
        "description": "Windowed backfills of candles and public trades, paged with a since-cursor.",
    },
    {
        "name": "snapshots",
        "title": "Live Snapshots",
        "description": "Point-in-time quotes and order-book depth, optionally across several exchanges.",
    },
    {
        "name": "derivatives",
        "title": "Perpetuals & Derivatives",
        "description": "Funding rates and open interest for perpetual swaps and futures.",
    },
    {
        "name": "reference",
        "title": "Reference Data",
        "description": "Exchanges, their listed markets and the CCXT capabilities each one supports.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


_DUCKDB_TYPES = {
    pa.string(): "VARCHAR",
    pa.float64(): "DOUBLE",
    pa.int64(): "BIGINT",
    pa.bool_(): "BOOLEAN",
    pa.timestamp("us"): "TIMESTAMP",
    pa.list_(pa.string()): "VARCHAR[]",
}


def result_columns(schema: pa.Schema) -> str:
    """Build the ``vgi.result_columns_schema`` tag for a schema from its field comments."""
    return json.dumps(
        [
            {
                "name": f.name,
                "type": _DUCKDB_TYPES[f.type],
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


CATALOG_DOC_LLM = (
    "Public cryptocurrency market data from more than one hundred centralized exchanges, through "
    "the CCXT library's unified API. Exchanges are named by their CCXT id (for example 'kraken', "
    "'coinbase', 'okx', 'binance', 'bybit') and markets by CCXT's unified symbol: 'BASE/QUOTE' for "
    "spot (such as 'BTC/USD') and 'BASE/QUOTE:SETTLE' for perpetual swaps (such as "
    "'BTC/USDT:USDT'). Use it for candle backfills, cross-exchange price comparison, order-book "
    "depth, recent trades, and perpetual funding and open interest. No API key is needed: only "
    "public endpoints are reachable, and every query is a live request to the exchange. Timestamps "
    "are UTC. Exchanges differ in what they support, so check capabilities before relying on one."
)

CATALOG_DOC_MD = (
    "# Crypto exchange market data\n\n"
    "Unified public market data from more than one hundred centralized crypto exchanges, served "
    "to DuckDB through the MIT-licensed [CCXT](https://github.com/ccxt/ccxt) library: candles, "
    "quotes, order books, trades, listed markets, and perpetual-swap funding and open interest.\n\n"
    "Exchanges are named by CCXT id and markets by CCXT's unified symbol (`BTC/USD` for spot, "
    "`BTC/USDT:USDT` for a USDT-settled perpetual). No credentials are used or accepted.\n\n"
    "**Terms of use.** " + TERMS_NOTE
)

SCHEMA_DOC_LLM = (
    "Table functions over CCXT's public market-data API. History functions page through a time "
    "window with a since-cursor and accept `TIMESTAMP`, `DATE` or ISO-8601 string bounds; snapshot functions "
    "return the current state and some accept a list of exchanges to compare them side by side, "
    "reporting a per-exchange failure in an error column instead of failing the query. Reference "
    "functions describe exchanges, markets and capabilities without fetching prices. A plain "
    "`TIMESTAMP` bound is read as UTC; for a relative window pass a `TIMESTAMPTZ` such as "
    "`now() - INTERVAL 1 DAY`, because `now()::TIMESTAMP` is local wall-clock time. The leading "
    "arguments are positional; the rest are named, written name := value. Funding rates are "
    "decimals per funding interval where positive means longs pay shorts, with an annualized "
    "column alongside."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Public crypto market data through CCXT.\n\n"
    "- **History** pages through a time window and streams one exchange page per batch.\n"
    "- **Snapshots** return the current quote or book, and can fan out across exchanges.\n"
    "- **Derivatives** cover perpetual funding (decimal per interval, positive = longs pay) and "
    "open interest.\n"
    "- **Reference** data lists exchanges, markets and per-exchange capabilities."
)

AGENT_TEST_TASKS = [
    {
        "name": "kraken_supports_ohlcv",
        "prompt": "Using the ccxt worker, does the Kraken exchange support fetching OHLCV candles?",
    },
    {
        "name": "exchanges_with_funding_history",
        "prompt": "Using the ccxt worker, how many exchanges support fetching funding rate history?",
    },
    {
        "name": "btc_daily_closes",
        "prompt": "Using the ccxt worker, list Coinbase's daily BTC/USD closing prices for the first week "
        "of January 2025, oldest first.",
    },
    {
        "name": "okx_funding_last_month",
        "prompt": "Using the ccxt worker, what was the average annualized funding rate of the "
        "BTC/USDT:USDT perpetual on OKX over the last 30 days?",
    },
    {
        "name": "btc_spread_kraken_coinbase",
        "prompt": "Using the ccxt worker, what is the current bid-ask spread of BTC/USD on Kraken and on Coinbase?",
    },
    {
        "name": "kraken_recent_trade_count",
        "prompt": "Using the ccxt worker, how many trades are in Kraken's most recent page of BTC/USD trades?",
    },
    {
        "name": "kraken_book_liquidity",
        "prompt": "Using the ccxt worker, how much BTC is resting in the top 10 bid levels of Kraken's BTC/USD book?",
    },
    {
        "name": "okx_btc_perpetuals",
        "prompt": "Using the ccxt worker, which BTC perpetual swaps does OKX list?",
    },
    {
        "name": "okx_current_funding",
        "prompt": "Using the ccxt worker, what is the annualized funding rate of OKX's BTC/USDT:USDT "
        "perpetual right now?",
    },
    {
        "name": "okx_open_interest",
        "prompt": "Using the ccxt worker, what is the open interest of OKX's BTC/USDT:USDT perpetual?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Crypto Market Data",
    "vgi.subtitle": "Candles, quotes, order books, trades and perpetual funding from 100+ crypto exchanges.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "MIT",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "crypto, cryptocurrency, bitcoin, ccxt, exchange, market data, ohlcv, candles, order book, "
        "funding rate, perpetual swap, open interest"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
