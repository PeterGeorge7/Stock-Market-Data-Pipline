WITH 
source AS (
    SELECT
        symbol,
        TRY_CAST(current_price AS DOUBLE) AS current_price_db1,
        market_timestamp
    FROM {{ ref('silver_clean_stock_quotes') }}
    WHERE TRY_CAST(current_price AS DOUBLE) IS NOT NULL
),
latest_day AS (
    SELECT
        CAST(TO_TIMESTAMP_LTZ(MAX(market_timestamp)) AS DATE) AS max_day
    FROM source
),
latest_prices AS (
    SELECT
        s.symbol,
        AVG(s.current_price_db1) AS avg_price
    FROM source s
    JOIN latest_day ld
    ON CAST(TO_TIMESTAMP_LTZ(s.market_timestamp) AS DATE) = ld.max_day
    GROUP BY s.symbol
),
all_time_volatility AS (
    SELECT
        symbol,
        STDDEV_POP(current_price_db1) AS volatility,
        CASE 
            WHEN AVG(current_price_db1) = 0 THEN NULL
            ELSE STDDEV_POP(current_price_db1) / NULLIF(AVG(current_price_db1), 0)
        END AS relative_volatility
    FROM source
    GROUP BY symbol
)
SELECT
    lp.symbol,
    lp.avg_price,
    atv.volatility,
    atv.relative_volatility
FROM latest_prices lp
JOIN all_time_volatility atv
ON lp.symbol = atv.symbol
ORDER BY lp.symbol