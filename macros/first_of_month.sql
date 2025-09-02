{% macro first_of_month(column) %}
    CASE
        -- If column matches MMM-YY (e.g., Jan-25)
        WHEN TRY_TO_DATE({{ column }}, 'MON-YY') IS NOT NULL THEN
            DATE_TRUNC('MONTH', TO_DATE({{ column }}, 'MON-YY'))
        -- Otherwise assume ISO format YYYY-MM-DD
        ELSE
            DATE_TRUNC('MONTH', TO_DATE({{ column }}, 'YYYY-MM-DD'))
    END
{% endmacro %}
