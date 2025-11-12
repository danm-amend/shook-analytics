{% macro union_wip_dynamic(control_table, db_name, schema_name) %}
    {% set control_query %}
        select table_name, file_month_year
        from {{ control_table }}
        order by table_name
    {% endset %}

    {% set results = run_query(control_query) %}
    {% if execute %}
        {% set rows = results.rows %}
    {% else %}
        {% set rows = [] %}
    {% endif %}

    {% set sql_parts = [] %}

    {% for row in rows %}
        {% set t = row[0] %}
        {% set file_month_year = row[1] %}

        {% set part %}
            select 
                *, 
                '{{ t }}' as source_table, 
                '{{ file_month_year }}' as file_month_year 
            from {{ db_name }}.{{ schema_name }}.{{ t }}
        {% endset %}

        {% do sql_parts.append(part) %}
    {% endfor %}

    {{ sql_parts | join(" union all ") }}
{% endmacro %}