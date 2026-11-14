# CCXT worker notebook

[01 · Funding rates need settlement intervals](01_funding_rates_need_intervals.ipynb) measures how a fixed interval assumption changes annualized funding rates, using the real worker's explicit synthetic exchange fixtures. It distinguishes unsupported history, an empty supported window, zero funding and nonzero observations.

## Run the saved example

Requirements: Python 3.13 or 3.14, [uv](https://docs.astral.sh/uv/), Git and network access for first installation. This directory can be copied and run without the worker checkout. From this directory:

```bash
uv sync --locked
uv run --locked python execute.py
```

The runner starts a fresh kernel, uses this directory as cwd, and replaces the saved notebook atomically only after success. Logs, environments, experiments and extracted charts are ignored. For an editor kernel:

```bash
uv run --locked python -m ipykernel install --user --name vgi-ccxt-notebooks --display-name 'Python (vgi-ccxt notebooks)'
```

Select that kernel, restart it and Run All. Imports, plotting and cleanup are folded; parameters and analytical SQL remain visible. [STANDARD.md](STANDARD.md) records the presentation convention.

## Portable worker pin

The visible connection uses:

```sql
ATTACH 'ccxt' AS ccxt (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-ccxt@v0.1.0 --with ccxt==4.5.92 --with vgi-python==0.51.1 --with vgi-rpc==0.53.1 vgi-ccxt-worker');
```

Tag `v0.1.0` resolved to `b8d02c64b618bd74c9bbea689de4c62897a69d78` during validation. Haybarn is locked to `2.0.0rc11`. uv caches the Git package; no root `.venv` or source directory is discovered. To upgrade, choose a published revision and compatible CCXT/SDK/RPC versions, then repeat the default, experiment and live validation before updating saved outputs.

## Data and interpretation

`fixtures.json` is copied unchanged from `test/fixtures/exchanges.json` at the pinned worker revision. It configures the worker's shipped `FakeExchange`, including synthetic rate generation, page caps, an interval transition, and a later-starting series. Exchange labels are identifiers for stand-ins, not historical exchange claims. The notebook explicitly sets `VGI_CCXT_FIXTURES`; fixture selection is never a silent response to a live failure. Its fingerprint and execution UTC bounds are shown in output.

The requested half-open interval is `[2024-01-26 00:00, 2024-02-05 00:00)` UTC. History is materialized once per supported fixture and reused for every table and chart. `max_rows` is a protective cap, not a substitute for time bounds. Settlement rates and their inferred intervals are compared with `rate × 8760 / hours`, simple annualization for a 365-day year. This is not a compounded return or a forecast. Spacing-based inference can be ambiguous when real settlement rows are missing.

The [CCXT manual](https://github.com/ccxt/ccxt/wiki/manual#funding-rate-history) describes its unified funding-history interface. Worker-specific normalization is documented in the [pinned history implementation](https://github.com/Query-farm/vgi-ccxt/blob/b8d02c64b618bd74c9bbea689de4c62897a69d78/src/ccxt_worker/history.py). No authenticated or write-capable method is used. Live endpoints can be unavailable or geographically restricted; current live values must never be presented as this synthetic series.

## One-parameter experiment

```bash
uv run --locked python execute.py --parameters '{"ASSUMED_HOURS":4}' --output-dir artifacts/assumed-4
```

Compare source fingerprints, the interval chart and the signed annualization difference. The worker data are unchanged; using four hours shifts the error to the eight-hour segment. Reset to `ASSUMED_HOURS = 8`. The runner rejects parameter overrides without a separate destination, same-destination overrides and unknown parameters; experiment copies include assets and setup companions.

## Validation

Default and detached notebooks-only experiment executions retain embedded figures. All four chart outputs were inspected. The repository's `make check` covers formatting, lint/type/doc checks, unit tests, SQL over stdio/launch/HTTP and executable metadata examples. A separate bounded public OKX funding-history probe used the same pinned command through Haybarn with `VGI_CCXT_FIXTURES` removed. Its live outcome is recorded in the rollout evidence; no live values enter this fixture notebook.
