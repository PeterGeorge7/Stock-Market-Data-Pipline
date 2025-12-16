with enriched AS (
    SELECT
        symbol,
        cast(market_timestamp as date) as trade_date,
        day_low,
        day_high,
        current_price,
        first_value(current_price) OVER (
            PARTITION BY symbol, CAST(market_timestamp AS date)
            ORDER BY market_timestamp
        ) AS candle_open,
        last_value(current_price) OVER (
            PARTITION BY symbol, CAST(market_timestamp AS date)
            ORDER BY market_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS candle_close
    FROM {{ ref("silver_clean_stock_quotes")}}
),
    candles AS (
        SELECT
            symbol,
            trade_date AS candle_time,
            MIN(day_low) AS candle_low,
            MAX(day_high) AS candle_high,
            any_value(candle_open) AS candle_open,
            any_value(candle_close) AS candle_close,
            avg(current_price) AS trend_line
        FROM enriched
        GROUP BY symbol, candle_time
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY candle_time DESC) AS rn
        FROM candles
    )

SELECT
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line  
FROM ranked
WHERE rn <= 12
ORDER BY symbol, candle_time