"""The single network chokepoint: every exchange call in this worker goes through here.

This is the only module in the package that imports ``ccxt``. It enforces three
rules, each covered by ``tests/test_readonly_guard.py``:

* **Public market data only.** ``PublicExchange`` exposes a fixed allowlist of
  CCXT methods (``ALLOWED_METHODS``): ``load_markets`` and public ``fetch_*``
  market-data calls. It has no ``__getattr__`` and never hands out the
  underlying CCXT object, so order entry, balances, withdrawals, transfers and
  every other private or trading method are unreachable from SQL.
* **No credentials.** Exchanges are instantiated with rate limiting and a
  timeout and nothing else: no ``apiKey``, ``secret``, ``password`` or
  ``uid`` is ever set.
* **No attribute access from SQL input.** An exchange id is checked against
  ``ccxt.exchanges`` (or the fixture file's ids) before it is used to look up
  an exchange class.

Each exchange is created once per process, with ``enableRateLimit=True``, and
kept in a pool, so CCXT's leaky-bucket limiter spans queries instead of being
reset by each one. Calls to one exchange are serialized by a lock for the same
reason.

Setting ``VGI_CCXT_FIXTURES`` to a JSON fixture file swaps every exchange for a
deterministic offline stand-in (``fixtures.FakeExchange``). The tests use it;
the allowlist and capability checks still apply to the stand-in.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import ccxt

from .fixtures import FakeExchange, load_fixture

LOG = logging.getLogger(__name__)

FIXTURES_ENV_VAR = "VGI_CCXT_FIXTURES"
TIMEOUT_MS = 30_000
MAX_ATTEMPTS = 3

#: The complete set of CCXT methods this worker may call, mapped to the CCXT
#: ``has`` capability that must be advertised before the call is attempted.
#: Every entry is an unauthenticated market-data endpoint. Never add a method
#: that needs an API key (``fetch_balance``, ``fetch_my_trades``,
#: ``fetch_funding_history``, ``fetch_positions``, ``create_order`` …).
ALLOWED_METHODS: Mapping[str, str] = {
    "load_markets": "fetchMarkets",
    "fetch_ohlcv": "fetchOHLCV",
    "fetch_ticker": "fetchTicker",
    "fetch_order_book": "fetchOrderBook",
    "fetch_trades": "fetchTrades",
    "fetch_funding_rate": "fetchFundingRate",
    "fetch_funding_rate_history": "fetchFundingRateHistory",
    "fetch_open_interest": "fetchOpenInterest",
}

_EXCHANGE_ID = re.compile(r"^[a-z][a-z0-9_]{0,40}$")

#: CCXT errors worth one more try after a pause; everything else fails fast.
_RETRYABLE: tuple[type[Exception], ...] = (
    ccxt.RateLimitExceeded,
    ccxt.DDoSProtection,
    ccxt.RequestTimeout,
    ccxt.ExchangeNotAvailable,
)


def _refused(err: Exception) -> bool:
    """Whether the exchange refused this caller outright (geo-block, ban): waiting will not help.

    CCXT reports HTTP 451 and 403 as ``ExchangeNotAvailable``, the same class as a 503 during
    maintenance, so the status code in the message is the only way to tell them apart.
    """
    text = str(err)
    return " 451 " in text or " 403 " in text or "restricted location" in text


class ExchangeCallError(RuntimeError):
    """An exchange call failed; the message names the exchange, the method and the CCXT error."""


class UnsupportedCapabilityError(ValueError):
    """The exchange does not advertise the CCXT capability a function needs."""


class UntilRequiredError(ExchangeCallError):
    """The exchange needs an explicit ``until`` whenever ``since`` is given (Coinbase trades)."""


class UnknownExchangeError(ValueError):
    """The exchange id is not one CCXT (or the fixture file) knows."""


def capability_status(value: Any) -> str:
    """Map a CCXT ``has`` value to ``native`` / ``emulated`` / ``unsupported`` / ``unknown``."""
    if value is True:
        return "native"
    if value == "emulated":
        return "emulated"
    if value is False:
        return "unsupported"
    return "unknown"


class PublicExchange:
    """A read-only view of one CCXT exchange: metadata plus allowlisted public calls."""

    __slots__ = ("_exchange", "_lock", "id")

    def __init__(self, exchange_id: str, exchange: Any) -> None:
        """Wrap ``exchange`` (a CCXT instance or a fixture stand-in)."""
        self.id = exchange_id
        self._exchange = exchange
        self._lock = threading.Lock()

    # ------------------------------------------------------------- metadata

    @property
    def name(self) -> str:
        """Display name, e.g. ``Kraken``."""
        return str(self._exchange.name)

    @property
    def countries(self) -> list[str]:
        """ISO country codes CCXT lists for the exchange."""
        return [str(c) for c in (self._exchange.countries or [])]

    @property
    def certified(self) -> bool:
        """Whether CCXT marks the integration as certified."""
        return bool(getattr(self._exchange, "certified", False))

    @property
    def pro(self) -> bool:
        """Whether CCXT Pro (WebSocket) supports the exchange."""
        return bool(getattr(self._exchange, "pro", False))

    @property
    def rate_limit_ms(self) -> float:
        """CCXT's minimum spacing between requests, in milliseconds."""
        return float(self._exchange.rateLimit)

    @property
    def website(self) -> str | None:
        """The exchange's home page, if CCXT records one."""
        www = (getattr(self._exchange, "urls", None) or {}).get("www")
        if isinstance(www, list):
            www = www[0] if www else None
        return str(www) if www else None

    @property
    def has(self) -> dict[str, Any]:
        """A copy of CCXT's capability map."""
        return dict(self._exchange.has)

    @property
    def timeframes(self) -> dict[str, Any]:
        """The OHLCV timeframes the exchange accepts (empty if none)."""
        return dict(self._exchange.timeframes or {})

    def status(self, capability: str) -> str:
        """``native`` / ``emulated`` / ``unsupported`` / ``unknown`` for one ``has`` key."""
        return capability_status(self._exchange.has.get(capability))

    def require(self, capability: str) -> None:
        """Raise ``UnsupportedCapabilityError`` unless ``capability`` is native or emulated."""
        status = self.status(capability)
        if status not in ("native", "emulated"):
            raise UnsupportedCapabilityError(
                f"exchange '{self.id}' does not support {capability} "
                f"(CCXT has['{capability}'] is {status}); "
                f"check ccxt.main.has('{self.id}') for what it does support"
            )

    def page_limit(self, capability: str, default: int) -> int:
        """Return the largest page CCXT documents for ``capability`` here, else ``default``."""
        # CCXT nests these as features[spot][fetchOHLCV] and features[swap][linear][fetchOHLCV].
        limits: list[int] = []
        pending: list[Any] = [getattr(self._exchange, "features", None) or {}]
        while pending:
            block = pending.pop()
            if not isinstance(block, dict):
                continue
            entry = block.get(capability)
            if isinstance(entry, dict) and isinstance(entry.get("limit"), int) and entry["limit"] > 0:
                limits.append(entry["limit"])
            pending.extend(v for k, v in block.items() if k != capability and isinstance(v, dict))
        return min(limits) if limits else default

    # ------------------------------------------------------------- calls

    def call(self, method: str, *args: Any, until_ms: int | None = None) -> Any:
        """Call one allowlisted public CCXT method, retrying transient failures.

        Args:
            method: A key of ``ALLOWED_METHODS``; anything else is refused.
            *args: Positional arguments for the CCXT method.
            until_ms: CCXT's unified ``until`` parameter, for exchanges that need
                an explicit window end. It is the only CCXT ``params`` entry this
                worker ever sets, always to an integer it computed itself; no
                caller-supplied ``params`` dict exists.

        Returns:
            The CCXT method's return value.

        Raises:
            PermissionError: ``method`` is not on the allowlist.
            UntilRequiredError: the exchange wants ``until_ms`` alongside ``since``.
            ExchangeCallError: the exchange call failed.

        """
        capability = ALLOWED_METHODS.get(method)
        if capability is None:
            raise PermissionError(f"'{method}' is not a public market-data method this worker may call")
        self.require(capability)
        fn: Callable[..., Any] = getattr(self._exchange, method)
        kwargs: dict[str, Any] = {} if until_ms is None else {"params": {"until": int(until_ms)}}
        attempt = 1
        while True:
            try:
                with self._lock:
                    return fn(*args, **kwargs)
            except ccxt.BaseError as err:
                if isinstance(err, ccxt.ArgumentsRequired) and "until" in str(err):
                    raise UntilRequiredError(self._describe(capability, err)) from err
                if not isinstance(err, _RETRYABLE) or _refused(err) or attempt >= MAX_ATTEMPTS:
                    raise ExchangeCallError(self._describe(capability, err)) from err
                # Offline fixtures have nothing to wait for; real exchanges get backoff.
                delay = 0.0 if _fixture_path() else min(2.0**attempt, 10.0)
                LOG.warning("%s %s: %s; retrying in %.0fs", self.id, capability, type(err).__name__, delay)
                time.sleep(delay)
                attempt += 1

    def _describe(self, capability: str, err: Exception) -> str:
        message = str(err).strip() or "no detail"
        if len(message) > 500:
            message = message[:500] + "…"
        return f"{self.id} {capability} failed: {type(err).__name__}: {message}"

    def markets(self) -> dict[str, Any]:
        """Load (once per process, CCXT caches it) and return the exchange's markets by symbol."""
        return dict(self.call("load_markets"))


# ----------------------------------------------------------------- the pool

_POOL: dict[str, PublicExchange] = {}
_POOL_LOCK = threading.Lock()


def _fixture_path() -> str | None:
    return os.environ.get(FIXTURES_ENV_VAR) or None


def exchange_ids() -> list[str]:
    """Every exchange id this worker accepts, sorted."""
    path = _fixture_path()
    if path is not None:
        return sorted(load_fixture(path)["exchanges"])
    return sorted(str(e) for e in ccxt.exchanges)


def validate_exchange_id(exchange_id: object) -> str:
    """Return ``exchange_id`` if it names a known exchange, else raise ``UnknownExchangeError``."""
    if not isinstance(exchange_id, str) or not exchange_id.strip():
        raise UnknownExchangeError("exchange must be a non-empty CCXT exchange id such as 'kraken'")
    candidate = exchange_id.strip().lower()
    known = exchange_ids()
    if not _EXCHANGE_ID.match(candidate) or candidate not in known:
        raise UnknownExchangeError(
            f"unknown exchange '{exchange_id}'; use a CCXT exchange id such as 'kraken', 'coinbase' "
            f"or 'okx' (SELECT id FROM ccxt.main.exchanges lists all {len(known)})"
        )
    return candidate


def _instantiate(exchange_id: str) -> Any:
    path = _fixture_path()
    if path is not None:
        return FakeExchange(exchange_id, load_fixture(path), errors=ccxt)
    # Safe: exchange_id was matched against ccxt.exchanges by validate_exchange_id.
    exchange_class = getattr(ccxt, exchange_id)
    return exchange_class({"enableRateLimit": True, "timeout": TIMEOUT_MS})


def get_exchange(exchange_id: object) -> PublicExchange:
    """Return the pooled read-only exchange for ``exchange_id`` (validated first)."""
    key = validate_exchange_id(exchange_id)
    with _POOL_LOCK:
        exchange = _POOL.get(key)
        if exchange is None:
            exchange = PublicExchange(key, _instantiate(key))
            _POOL[key] = exchange
        return exchange


def now_ms() -> int:
    """Return the time in epoch ms (the fixture's fixed clock in fixture mode)."""
    path = _fixture_path()
    if path is not None:
        return int(load_fixture(path)["now_ms"])
    return int(time.time() * 1000)


def parse_timeframe_ms(timeframe: str) -> int:
    """Length of a CCXT timeframe string such as ``1h`` in milliseconds."""
    return int(ccxt.Exchange.parse_timeframe(timeframe)) * 1000


def reset_pool() -> None:
    """Forget every pooled exchange (tests switch fixture files with this)."""
    with _POOL_LOCK:
        _POOL.clear()
