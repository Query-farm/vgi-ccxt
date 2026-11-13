"""Assemble the functions into a catalog and run the worker.

Functions live in ``history.py`` (since-cursor backfills), ``snapshots.py``
(current state, with cross-exchange fan-out) and ``reference.py`` (exchanges,
markets, capabilities). Every exchange call goes through ``client.py``.

Run it:

    vgi-ccxt-worker                      # stdio: what DuckDB launches by default
    vgi-ccxt-worker --http --port 8000   # HTTP: for Docker / hosted deploys
    vgi-ccxt-worker --help

Attach it from DuckDB:

    ATTACH 'ccxt' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-ccxt-worker');
"""

from __future__ import annotations

import json
from typing import Any

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.constraints import constraint_record

from .history import FundingRates, Ohlcv, Trades
from .meta import (
    CATALOG_DOC_MD,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .reference import Exchanges, Has, Markets
from .snapshots import FundingRate, OpenInterest, OrderBook, Ticker

FUNCTIONS: list[type[Any]] = [
    Ohlcv,
    Trades,
    Ticker,
    OrderBook,
    Markets,
    Has,
    FundingRates,
    FundingRate,
    OpenInterest,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries`` (with descriptions).

    Overloads share one SQL name, so their examples are merged per name.
    Returns every example, for the schema-level list.
    """
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries([*FUNCTIONS, Exchanges])


def _schema_examples() -> list[dict[str, str]]:
    """One example per object for the schema-level list, which agent tools cap at 4,000 characters."""
    seen: set[str] = set()
    picked: list[dict[str, str]] = []
    objects: list[type[Any]] = [*FUNCTIONS, Exchanges]
    for fn in objects:
        if fn.Meta.name not in seen and fn.Meta.examples:
            seen.add(fn.Meta.name)
            ex = fn.Meta.examples[0]
            picked.append({"description": ex.description, "sql": ex.sql})
    return picked


class CcxtWorker(Worker):
    """The worker process: one catalog, one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DOC_MD,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_DOC_MD,
                functions=FUNCTIONS,
                tables=[
                    Table(
                        name="exchanges",
                        function=Exchanges,
                        comment=Exchanges.Meta.description,
                        tags={**Exchanges.Meta.tags, "domain": "finance"},
                        column_comments={
                            f.name: (f.metadata or {})[b"comment"].decode() for f in Exchanges.FIXED_SCHEMA
                        },
                        constraints=(
                            constraint_record(
                                ["main"],
                                "exchanges",
                                "pk",
                                "PRIMARY_KEY",
                                [["id"]],
                                enforcement="ENFORCED",
                                validation="ALL_ROWS",
                            ),
                            constraint_record(
                                ["main"],
                                "exchanges",
                                "id_not_null",
                                "NOT_NULL",
                                [["id"]],
                                enforcement="ENFORCED",
                                validation="ALL_ROWS",
                            ),
                        ),
                    )
                ],
                tags={
                    "vgi.title": "Crypto Market Data Functions",
                    "domain": "finance",
                    "vgi.keywords": keywords_array(
                        "crypto, ohlcv, ticker, order book, trades, funding rate, open interest, markets"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_schema_examples()),
                },
            )
        ],
    )


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    CcxtWorker.main()


if __name__ == "__main__":
    main()
