# Changelog

## 0.1.0 (2026-11-13)

First release of the CCXT worker: public crypto-exchange market data as DuckDB
table functions.

### Added

- `ohlcv`, `trades` and `funding_rates`: windowed backfills over `[since, until)`
  (`[start_date, end_date)` for funding), one exchange page per Arrow batch. The
  position lives in a serializable cursor, so a backfill continues correctly over
  the HTTP transport, where each page can be served by a different process.
  `trades` keeps fills that share a millisecond intact across page boundaries, and
  walks self-sizing time windows on exchanges that require an explicit `until`
  (Coinbase).
- `ticker`, `funding_rate` and `open_interest` take one exchange id or a list and
  fan out, one row per exchange, reporting a per-exchange failure in an `error`
  column.
- `order_book`, `markets`, `has`, and an `exchanges` table (`SELECT * FROM
  ccxt.main.exchanges`).
- Funding is normalized to a decimal rate per interval with the sign convention
  positive = longs pay shorts, an `interval_hours` inferred from the spacing of
  settlements (exchanges switch between 8, 4 and 1 hours), and a simple
  annualized rate.
- A warning (client log) when an exchange's history starts later than the
  requested start, instead of a silently short result.

### Security

- Read-only by construction: `client.py` is the only module that imports `ccxt`,
  and it calls nothing outside an allowlist of public market-data methods. No
  credentials are accepted or set. Exchange ids are validated against
  `ccxt.exchanges` before any attribute lookup. `tests/test_readonly_guard.py`
  enforces all three.

### Testing

- `VGI_CCXT_FIXTURES` swaps every exchange for a deterministic offline stand-in.
  Unit tests, SQL tests over stdio/launch/http, the metadata check and
  `vgi-worker.json` (stock DuckDB verification) all run against it.
- `make live-check` runs a gentle smoke test against Kraken, Coinbase and OKX.

### Operational

- Removed the starter template's example functions and `examples/` recipes,
  which also removed the template's two mypy baseline errors.
- vgi-lint gates at `fail_on = "info"`; score 100, no findings.

Targets: vgi-python 0.51 · VGI protocol vgi.v3 3.3 · ccxt 4.5 · Haybarn 2.0.0rc11 · DuckDB 2.0.0
