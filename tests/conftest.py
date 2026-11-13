"""Shared fixtures: every test runs against the offline fixture exchanges, never the network."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arguments
from vgi.client import Client

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures" / "exchanges.json"

# Set before the worker package is imported or launched: the chokepoint reads it per call,
# and the client's subprocess inherits it.
os.environ["VGI_CCXT_FIXTURES"] = str(FIXTURES)

SCHEMA = ["main"]


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "ccxt_worker.worker"]) as c:
        yield c


def scan(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    """Run table function ``name`` and collect every batch."""
    arguments = Arguments(
        positional=tuple(v if isinstance(v, pa.Scalar) else pa.scalar(v) for v in positional),
        named={k: v if isinstance(v, pa.Scalar) else pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments))
    return pa.Table.from_batches(batches) if batches else pa.table({})


def batch_count(client: Client, name: str, *positional: Any, **named: Any) -> int:
    """How many non-empty batches ``name`` streamed."""
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments)
    return sum(1 for b in batches if b.num_rows)
