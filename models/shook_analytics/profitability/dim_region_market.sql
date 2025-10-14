WITH
department_base AS (
    SELECT DISTINCT
        region_market_clean,
        region_clean,
        market_clean
    FROM
        {{ ref('gl_actuals') }}
), region_market_numbers AS (
    SELECT
        *,
        CASE
            WHEN region_clean = 'Great Lakes' THEN '04'
            WHEN region_clean = 'Central' THEN '06'
            WHEN region_clean = 'Mid-Atlantic' THEN '08'
            WHEN region_clean = 'Midwest' THEN '12'
            --ELSE 'Other'
        END AS region_number,
        CASE
            WHEN market_clean = 'Water' THEN '10'
            WHEN market_clean = 'Education' THEN '20'
            WHEN market_clean = 'Healthcare' THEN '30'
            WHEN market_clean = 'Industrial' THEN '40'
            --ELSE 'Other'
        END AS market_number
    FROM department_base
)
SELECT 
    *,
    concat(region_number, market_number) as department_number
FROM 
    region_market_numbers