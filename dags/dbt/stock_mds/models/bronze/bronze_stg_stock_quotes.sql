SELECT
    v:c::FLOAT AS current_price,
    v:d::FLOAT AS change_amount,
    v:dp::FLOAT AS change_percent,
    v:h::FLOAT AS day_high,
    v:l::FLOAT AS day_low,
    v:o::FLOAT AS day_open,
    v:pc::FLOAT AS prev_close,
    v:t::TIMESTAMP AS market_timestamp,
    v:symbol::STRING AS symbol,
    v:fetched_at::TIMESTAMP AS fetched_at
FROM {{ source('raw', 'BRONZE_STOCK_QUOTES_RAW') }}