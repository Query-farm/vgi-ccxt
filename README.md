<p align="center">
  <a href="https://query.farm/vgi/">
    <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI) logo" width="320">
  </a>
</p>

<p align="center"><em>A Query.Farm VGI worker for DuckDB.</em></p>

# Query Crypto Exchange Prices, Order Books & Funding Rates in DuckDB

> **vgi-ccxt** · a [Query.Farm](https://query.farm) VGI worker · powered by [CCXT](https://github.com/ccxt/ccxt)

[![CI](https://github.com/Query-farm/vgi-ccxt/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-ccxt/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Public market data from more than 100 centralized crypto exchanges as ordinary DuckDB
table functions: OHLCV candles with windowed backfill, cross-exchange tickers, order-book
depth, public trades, listed markets and capabilities, and perpetual-swap funding rates
and open interest. CCXT normalizes Binance, Coinbase, Kraken, OKX, Bybit and the rest into
one `symbol` / `timeframe` / `since` shape; this worker puts that shape behind SQL.

> **No credentials, public data only.** The worker can only call public market-data
> endpoints. Read-only is enforced structurally: `src/ccxt_worker/client.py` is the only
> module that imports `ccxt`, it calls nothing outside a fixed allowlist of public
> `fetch*` methods, it never sets an API key, and an exchange id from SQL is checked
> against `ccxt.exchanges` before it is used to look anything up.
> `tests/test_readonly_guard.py` fails the build if any of that stops being true.

> **Terms of use.** Every query is a live request from your machine to the exchange.
> Nothing is cached or redistributed by this worker; you are the API caller, and each
> exchange's terms and geographic restrictions apply to you. Binance.com, for example,
> answers US IP addresses with HTTP 451, and Bybit with 403. See [NOTICE](NOTICE).

## Quick start

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/). In DuckDB 2.0:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'ccxt' AS ccxt (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-ccxt@v0.1.0 vgi-ccxt-worker');
```

`uvx` fetches and caches the worker on first use; change `@v0.1.0` to move to a newer release.

In [Haybarn](https://query.farm/products/haybarn) use `INSTALL vgi FROM community; LOAD vgi;`
and the same `ATTACH`.

### Other ways to run it

```sql
-- From a local checkout (make build first); use an absolute path.
ATTACH 'ccxt' (TYPE vgi, LOCATION '/abs/path/to/vgi-ccxt/.venv/bin/vgi-ccxt-worker');

-- A worker served over HTTP (make run-http, or the Docker image); also LOAD httpfs.
ATTACH 'ccxt' (TYPE vgi, LOCATION 'http://127.0.0.1:8000');
```

## Examples

```sql
-- Hourly candles, backfilled through a since-cursor (one exchange page per batch).
SELECT ts, open, high, low, close, volume
FROM ccxt.main.ohlcv('coinbase', 'BTC/USD', '1h',
                     since := TIMESTAMP '2025-01-01', until := TIMESTAMP '2025-01-15')
ORDER BY ts;

-- One row per exchange; a failing venue reports why in `error`.
SELECT exchange, last, ask - bid AS spread, quote_volume, error
FROM ccxt.main.ticker(['kraken', 'coinbase', 'okx'], 'BTC/USDT');

-- Top-of-book depth.
SELECT side, level, price, amount
FROM ccxt.main.order_book('kraken', 'BTC/USD', depth := 20)
ORDER BY side, level;

-- Perpetual funding: decimal per interval (positive = longs pay shorts), plus annualized.
SELECT ts, funding_rate, interval_hours, funding_rate_annualized
FROM ccxt.main.funding_rates('okx', 'BTC/USDT:USDT', start_date := current_date - INTERVAL 30 DAY)
ORDER BY ts;

-- Which exchanges can answer a derivatives question?
SELECT id, name FROM ccxt.main.exchanges
WHERE has_funding_rate_history AND has_open_interest ORDER BY id;
```

## SQL surface

The leading arguments are positional; the rest are named (`name := value`). Timestamps
are UTC `TIMESTAMP`s; window bounds also accept a `TIMESTAMPTZ`, a `DATE` or an ISO-8601
string. A naive timestamp is read as UTC, so for a relative window write
`since := now() - INTERVAL 1 DAY` (a `TIMESTAMPTZ`), not `now()::TIMESTAMP - …`, which is
local wall-clock time and would shift the window by your UTC offset.

| Function | Returns |
|---|---|
| `ohlcv(exchange, symbol, timeframe, since :=, until :=, max_rows := 100000)` | `exchange, symbol, timeframe, ts, open, high, low, close, volume` |
| `trades(exchange, symbol, since :=, until :=, max_rows := 100000)` | `exchange, symbol, trade_id, ts, side, price, amount, cost, taker_or_maker` |
| `funding_rates(exchange, symbol, start_date :=, end_date :=, max_rows := 100000)` | `exchange, symbol, ts, funding_rate, interval_hours, funding_rate_annualized` |
| `ticker(exchange \| [exchange, …], symbol)` | `exchange, symbol, ts, last, bid, ask, high, low, open, base_volume, quote_volume, change_pct, vwap, error` |
| `funding_rate(exchange \| [exchange, …], symbol)` | `exchange, symbol, ts, funding_rate, interval_hours, funding_rate_annualized, next_funding_ts, mark_price, index_price, error` |
| `open_interest(exchange \| [exchange, …], symbol)` | `exchange, symbol, ts, open_interest_amount, open_interest_value, error` |
| `order_book(exchange, symbol, depth := 20)` | `exchange, symbol, ts, side, level, price, amount` |
| `markets(exchange)` | `exchange, symbol, market_id, base, quote, settle, type, contract, linear, inverse, contract_size, expiry, active, maker, taker, price_precision, amount_precision, min_amount, min_cost` |
| `has(exchange \| [exchange, …])` | `exchange, method, supported, status, exposed_by_worker` |
| table `exchanges` | `id, name, countries, certified, pro, rate_limit_ms, website, has_ohlcv, has_trades, has_funding_rate_history, has_open_interest` |

**Symbols** are CCXT's unified form: `BTC/USD` for spot, `BTC/USDT:USDT` for a
USDT-settled perpetual, `BTC/USD:BTC` for a coin-margined one. `markets(exchange)` lists
them.

**Windows** are half-open, `[since, until)`. Without `since` (or `start_date`) a history
function returns just the exchange's latest page. `until` defaults to now, fixed once when
the scan starts.

**Fan-out.** `ticker`, `funding_rate` and `open_interest` visit the exchanges one at a time
and never fail the whole query for one venue: an unsupported capability, an unlisted
symbol, a geo-block or an outage becomes that row's `error`. Unknown exchange ids are still
an error at bind.

**Capabilities.** A single-exchange function checks CCXT's `has` map at bind and fails with
a message naming the missing capability, for example
`exchange 'kraken' does not support fetchFundingRateHistory`. `has(exchange)` shows the
whole map, and `exposed_by_worker` marks the methods this worker calls.

### Funding rates

`funding_rate` is the decimal rate for one funding interval as the exchange publishes it
(`0.0001` = 0.01%), with the convention **positive = longs pay shorts**. CCXT passes each
exchange's own sign through; Binance, Bybit and OKX document this convention and no
exchange with the reverse sign is known. `FLIPPED_SIGN_EXCHANGES` in
`src/ccxt_worker/history.py` is the hook if one turns up.

`interval_hours` is inferred from the spacing between consecutive settlements, because
venues use 8, 4 or 1 hours and some switch per symbol. A gap that is not a standard
interval (a missing settlement) keeps the previous interval. A lone settlement takes the
interval the exchange declares for the current rate. `funding_rate_annualized` is
`funding_rate × 8760 / interval_hours`, simple rather than compounded.

## Exchange quirks

Measured on 2026-11-12 from a US address with `make live-check` and ad-hoc probes:

- **Kraken** serves only its latest 720 candles per timeframe, so an old `since` returns
  nothing (1h: the last 30 days). Tickers and books carry no timestamp (`ts` is NULL).
- **Coinbase** rejects a trades `since` without an `until`. The worker detects that and
  walks explicit time windows, halving a window that comes back full.
- Funding-history retention varies: **OKX** returned about three months, **KuCoin
  Futures** about one, **Kraken Futures** (hourly funding) about a year; **Gate** refuses a
  start older than 180 days, and **Bitget** ignores `since` and returns its latest page.
  **Hyperliquid** (hourly funding) returned January 2025 on request. Binance's depth was
  not measured: it is geo-blocked from the test machine. When an exchange starts later
  than requested, the worker logs a warning naming the first row it got.
- **Binance** (HTTP 451) and **Bybit** (HTTP 403 from CloudFront, which CCXT labels
  `RateLimitExceeded`) refuse US addresses. The worker does not retry a refusal.

## Development

```bash
make build        # install into .venv
make check        # everything CI runs: lint, unit tests, SQL tests over stdio/launch/http, vgi-lint
make live-check   # opt-in smoke test against Kraken, Coinbase and OKX (public GETs)
make lint-live    # vgi-lint against the real exchanges instead of the fixtures
```

```
src/ccxt_worker/
  client.py      # the network chokepoint: allowlist, exchange pool, retries
  fixtures.py    # deterministic offline stand-in exchanges (VGI_CCXT_FIXTURES)
  history.py     # ohlcv, trades, funding_rates: since-cursor backfills
  snapshots.py   # ticker, funding_rate, open_interest (fan-out), order_book
  reference.py   # markets, has, the exchanges table
  common.py      # argument parsing and funding normalization
  meta.py        # catalog/schema metadata
  worker.py      # assembles the catalog
tests/           # pytest; test_readonly_guard.py, test_live.py (opt-in)
test/sql/        # sqllogictest, run through Haybarn over every transport
test/fixtures/   # the fixture exchanges
vgi-worker.json  # stock-DuckDB verification manifest (fixtures)
```

**Offline by default.** `VGI_CCXT_FIXTURES=test/fixtures/exchanges.json` swaps every
exchange for a stand-in whose values are computed from row indexes, so tests assert exact
numbers. Pages are capped small so every backfill spans several pages, one stand-in
ignores `since`, one requires `until`, one fails, and one switches funding interval
mid-series. The unit tests, the SQL tests, the metadata check and `vgi-worker.json` all
use it; only `make live-check` and `make lint-live` touch the network.

**Rate limits.** Each exchange is created once per worker process with CCXT's
`enableRateLimit`, and calls to one exchange are serialized, so CCXT's limiter spans
queries. Rate-limit, timeout and maintenance errors are retried with backoff; refusals
(HTTP 451/403) and bad requests are not.

## Support

Open an issue on [this repository](https://github.com/Query-farm/vgi-ccxt/issues). For VGI
itself, see [query.farm](https://query.farm/vgi/).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the [MIT License](LICENSE).

Copyright 2026 Query Farm LLC - https://query.farm

The market data this worker returns belongs to the exchanges, is not covered by that
license, and is subject to each exchange's terms. This project is not affiliated with or
endorsed by CCXT or any exchange. See [NOTICE](NOTICE).
