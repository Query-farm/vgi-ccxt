"""Structural guard: this worker can only ever read public market data.

CCXT's exchange objects carry order entry, withdrawals, transfers and account
endpoints right next to the public market-data calls. The worker is read-only by
construction: ``client.py`` is the only module that touches ``ccxt``, and its
``PublicExchange`` wrapper calls nothing outside ``ALLOWED_METHODS``. These tests
fail the build if either stops being true.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path
from typing import Any

import ccxt
import pytest

from ccxt_worker import client
from ccxt_worker.client import ALLOWED_METHODS, PublicExchange, UnknownExchangeError, validate_exchange_id

PACKAGE = Path(client.__file__).resolve().parent

#: Substrings of CCXT method names that need credentials or move funds.
PRIVATE_MARKERS = (
    "order",
    "balance",
    "mytrades",
    "deposit",
    "withdraw",
    "transfer",
    "position",
    "ledger",
    "leverage",
    "margin",
    "borrow",
    "repay",
    "fundinghistory",
    "account",
    "tradingfee",
    "convert",
    "address",
    "liquidation",
    "settlement",
)

#: Private and trading methods that must never be reachable, as CCXT spells them.
FORBIDDEN_CALLS = [
    "create_order",
    "cancel_order",
    "cancel_all_orders",
    "edit_order",
    "fetch_balance",
    "fetch_my_trades",
    "fetch_open_orders",
    "fetch_positions",
    "fetch_funding_history",
    "fetch_ledger",
    "fetch_deposit_address",
    "withdraw",
    "transfer",
    "set_leverage",
    "sign",
    "request",
    "fetch",
]


def _modules() -> list[Path]:
    return sorted(PACKAGE.rglob("*.py"))


def test_only_the_chokepoint_imports_ccxt() -> None:
    importers = []
    for path in _modules():
        for node in ast.walk(ast.parse(path.read_text())):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            if any(n == "ccxt" or n.startswith("ccxt.") for n in names):
                importers.append(path.name)
    assert importers == ["client.py"]


def test_allowlist_is_public_market_data_only() -> None:
    for method, capability in ALLOWED_METHODS.items():
        assert method == "load_markets" or method.startswith("fetch_"), method
        # An order *book* is public market data; orders are not.
        lowered = (method + capability).lower().replace("_", "").replace("orderbook", "")
        assert not any(marker in lowered for marker in PRIVATE_MARKERS), method
        assert method not in FORBIDDEN_CALLS


def test_allowlisted_methods_exist_in_ccxt() -> None:
    for method, capability in ALLOWED_METHODS.items():
        assert callable(getattr(ccxt.Exchange, method)), method
        assert capability in ccxt.Exchange().has or capability == "fetchMarkets", capability


def test_every_call_site_names_an_allowlisted_method() -> None:
    """``.call("x", ...)`` literals across the package must all be on the allowlist."""
    for path in _modules():
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "call":
                first = node.args[0] if node.args else None
                assert isinstance(first, ast.Constant), f"{path.name}: .call() with a computed method name"
                assert first.value in ALLOWED_METHODS, f"{path.name}: .call({first.value!r})"


class Spy:
    """An exchange whose every method records that it ran."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.has = {"fetchBalance": True, "createOrder": True, "fetchTicker": True, "withdraw": True}

    def __getattr__(self, name: str) -> Any:
        def method(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            return {}

        return method


@pytest.mark.parametrize("method", FORBIDDEN_CALLS)
def test_private_and_trading_methods_are_refused_before_reaching_ccxt(method: str) -> None:
    spy = Spy()
    wrapper = PublicExchange("spy", spy)
    with pytest.raises(PermissionError, match="not a public market-data method"):
        wrapper.call(method, "BTC/USD")
    assert spy.calls == []


def test_wrapper_has_no_passthrough() -> None:
    wrapper = PublicExchange("spy", Spy())
    assert not hasattr(PublicExchange, "__getattr__")
    for method in FORBIDDEN_CALLS:
        assert not hasattr(wrapper, method), method
    with pytest.raises(AttributeError):
        wrapper.arbitrary = 1  # type: ignore[attr-defined]


def test_an_allowed_call_still_requires_the_capability() -> None:
    spy = Spy()
    spy.has = {"fetchTicker": False}
    with pytest.raises(ValueError, match="does not support fetchTicker"):
        PublicExchange("spy", spy).call("fetch_ticker", "BTC/USD")
    assert spy.calls == []


def test_real_exchanges_are_created_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(client.FIXTURES_ENV_VAR, raising=False)
    for exchange_id in ("kraken", "coinbase", "okx", "binance"):
        exchange = client._instantiate(exchange_id)  # constructor only; no network
        assert exchange.enableRateLimit is True
        assert exchange.timeout == client.TIMEOUT_MS
        for credential in ("apiKey", "secret", "password", "uid", "privateKey", "walletAddress", "token"):
            assert not getattr(exchange, credential, None), credential


@pytest.mark.parametrize(
    "hostile",
    ["exchanges", "Exchange", "__class__", "version", "base", "kraken.fetch_balance", "../kraken", "", "  ", "KRAKEN!"],
)
def test_exchange_ids_are_validated_before_attribute_lookup(hostile: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(client.FIXTURES_ENV_VAR, raising=False)
    with pytest.raises(UnknownExchangeError):
        validate_exchange_id(hostile)


def test_real_exchange_ids_are_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(client.FIXTURES_ENV_VAR, raising=False)
    assert validate_exchange_id("Kraken") == "kraken"
    assert len(client.exchange_ids()) == len(ccxt.exchanges)


def test_fixture_mode_is_opt_in() -> None:
    assert os.environ.get(client.FIXTURES_ENV_VAR), "the test suite must run against fixtures"


class Flaky:
    """Fails with a retryable CCXT error ``failures`` times, then answers."""

    def __init__(self, failures: int, error: type[Exception]) -> None:
        self.failures, self.error, self.attempts = failures, error, 0
        self.has = {"fetchTicker": True}

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        self.attempts += 1
        if self.attempts <= self.failures:
            raise self.error("try later")
        return {"symbol": symbol}


def test_transient_errors_are_retried() -> None:
    flaky = Flaky(2, ccxt.RateLimitExceeded)
    assert PublicExchange("flaky", flaky).call("fetch_ticker", "BTC/USD") == {"symbol": "BTC/USD"}
    assert flaky.attempts == 3


def test_retries_give_up_with_a_named_error() -> None:
    flaky = Flaky(9, ccxt.ExchangeNotAvailable)
    with pytest.raises(client.ExchangeCallError, match="flaky fetchTicker failed: ExchangeNotAvailable: try later"):
        PublicExchange("flaky", flaky).call("fetch_ticker", "BTC/USD")
    assert flaky.attempts == client.MAX_ATTEMPTS


def test_permanent_errors_are_not_retried() -> None:
    flaky = Flaky(9, ccxt.BadSymbol)
    with pytest.raises(client.ExchangeCallError, match="BadSymbol"):
        PublicExchange("flaky", flaky).call("fetch_ticker", "BTC/USD")
    assert flaky.attempts == 1


class GeoBlocked:
    """Answers every ticker request the way Binance answers a US IP."""

    def __init__(self) -> None:
        self.attempts = 0
        self.has = {"fetchTicker": True}

    def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        self.attempts += 1
        raise ccxt.ExchangeNotAvailable('binance GET https://api.binance.com 451 {"msg": "restricted location"}')


def test_geo_blocks_are_not_retried() -> None:
    blocked = GeoBlocked()
    with pytest.raises(client.ExchangeCallError, match="451"):
        PublicExchange("binance", blocked).call("fetch_ticker", "BTC/USDT")
    assert blocked.attempts == 1
