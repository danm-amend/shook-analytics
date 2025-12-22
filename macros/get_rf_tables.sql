-- deprecated
{% macro get_rf_tables() %}
    {% set sql %}
        WITH
        last_table_loads AS (
            select 
                *
                , MAX(last_load_dt) OVER (PARTITION BY table_name) AS table_last_load_dt
            from 
                shookdw.metadata.rolling_forecast_file_control
        ),
        tables_to_use AS (
            select
                *
            from 
                last_table_loads
            where
                last_load_dt = table_last_load_dt
        )
        select
            table_name, file_month_year
        from
            tables_to_use
    {% endset %}

    {% if execute %}
        {% set results = run_query(sql) %}

        {% if results|length == 0 %}
            {{ exceptions.raise_compiler_error("No active tables found in control table") }}
    {% endif %}

    {# Build UNION ALL of all active tables #}
    {% set union_query_parts = [] %}

    -- {% for table_name in results.columns[0].values() %}
    --     {% set full_table_name = 'shookdw.sharepoint.' ~ table_name %}
    --     {% do union_query_parts.append('select * from ' ~ full_table_name) %}
    -- {% endfor %}

    {# results.columns[0] = table_name, results.columns[1] = file_month_year #}
    {% for i in range(results.rowcount) %}
        {% set table_name = results.columns[0].values()[i] %}
        {% set file_month_year = results.columns[1].values()[i] %}
        {% set full_table_name = 'shookdw.sharepoint.' ~ table_name %}

        {# Quote file_month_year as a string literal; adjust type casting if needed #}
        {% set select_stmt -%}
            select
                t.*,
                '{{ table_name }}'       as source_table_name,
                '{{ file_month_year }}' as source_file_month_year
            from {{ full_table_name }} as t
        {%- endset %}

        {% do union_query_parts.append(select_stmt) %}

    {% endfor %}

    {{ return(union_query_parts | join(' union all ')) }}

    {% else %}
    {# Fallback for parsing/compilation time #}
        {{ return("select * from shookdw.sharepoint.rolling_forecast_jun_2025") }}
    {% endif %}
{% endmacro %}