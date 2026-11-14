"""Examples that combine two or more of the worker's functions in one query.

Each one is defined once here, listed on the function that drives it (within its first
five examples, which is what agent tools show) and again in the schema-level list.
"""

from __future__ import annotations

from vgi.metadata import FunctionExample

FUNDING_OI_WEIGHTED = FunctionExample(
    sql=(
        "SELECT sum(f.funding_rate_annualized * o.open_interest_value) / sum(o.open_interest_value) "
        "AS oi_weighted_apr, sum(o.open_interest_value) AS total_oi_usdt, count(*) AS venues "
        "FROM ccxt.main.funding_rate(['binance', 'okx'], 'BTC/USDT:USDT') AS f "
        "JOIN ccxt.main.open_interest(['binance', 'okx'], 'BTC/USDT:USDT') AS o USING (exchange) "
        "WHERE f.error IS NULL AND o.error IS NULL"
    ),
    description=(
        "Current annualized BTC perpetual funding across Binance and OKX, weighted by each venue's "
        "open interest (funding_rate joined to open_interest)"
    ),
)

ORDER_BOOK_FILL_COST = FunctionExample(
    sql=(
        "WITH asks AS (SELECT price, amount, sum(amount) OVER (ORDER BY level) - amount AS ahead "
        "FROM ccxt.main.order_book('kraken', 'BTC/USD', depth := 50) WHERE side = 'ask'), "
        "fill AS (SELECT sum(least(amount, greatest(2 - ahead, 0))) AS filled_btc, "
        "sum(price * least(amount, greatest(2 - ahead, 0))) AS cost_usd FROM asks) "
        "SELECT fill.filled_btc, fill.cost_usd / fill.filled_btc AS avg_fill_price, "
        "fill.cost_usd * (1 + m.taker) AS cost_with_taker_fee_usd "
        "FROM fill JOIN ccxt.main.markets('kraken') AS m ON m.symbol = 'BTC/USD'"
    ),
    description=(
        "What a 2 BTC market buy on Kraken would cost: walk the ask side of the order book, then add "
        "the market's default taker fee from markets (filled_btc is below 2 if the book is too thin)"
    ),
)

FUNDING_VS_PRICE = FunctionExample(
    sql=(
        "WITH funding AS (SELECT CAST(ts AS DATE) AS day, sum(funding_rate) AS daily_funding "
        "FROM ccxt.main.funding_rates('binance', 'BTC/USDT:USDT', "
        "start_date := DATE '2025-01-01', end_date := DATE '2025-01-08') GROUP BY day), "
        "candles AS (SELECT CAST(ts AS DATE) AS day, close / open - 1 AS price_return "
        "FROM ccxt.main.ohlcv('binance', 'BTC/USDT:USDT', '1d', "
        "since := TIMESTAMP '2025-01-01', until := TIMESTAMP '2025-01-08')) "
        "SELECT day, daily_funding, price_return FROM funding JOIN candles USING (day) ORDER BY day"
    ),
    description=(
        "Funding settled each UTC day on Binance's BTC perpetual next to that day's open-to-close "
        "return of the perpetual itself, first week of 2025 (funding_rates joined to ohlcv)"
    ),
)

COMPOSED_EXAMPLES = [FUNDING_OI_WEIGHTED, ORDER_BOOK_FILL_COST, FUNDING_VS_PRICE]
