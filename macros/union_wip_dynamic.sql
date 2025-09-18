{% macro union_wip_dynamic(control_table, db_name, schema_name) %}
    {% set table_names = dbt_utils.get_column_values(table=control_table, column='table_name') %}
    {% set file_month_years = dbt_utils.get_column_values(table=control_table, column='file_month_year') %}

    {% set sql_parts = [] %}

    {% for t in table_names %}
        {% set file_month_year = file_month_years[loop.index0] %}
        {% do sql_parts.append(
            "select *, '" ~ t ~ "' as source_table, '" ~ file_month_year ~ "' as file_month_year from "
            ~ db_name ~ "." ~ schema_name ~ "." ~ t
        ) %}
    {% endfor %}

    {{ sql_parts | join(" union all ") }}
{% endmacro %}