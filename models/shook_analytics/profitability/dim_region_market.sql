SELECT DISTINCT
    region_market_clean,
    region_clean,
    market_clean
FROM
    {{ ref('gl_actuals') }}