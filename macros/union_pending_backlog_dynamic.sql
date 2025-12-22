-- deprecated
{% macro union_pending_backlog_dynamic(control_table, db_name, schema_name) %}
    {% set table_names = dbt_utils.get_column_values(table=control_table, column='table_name') %}
    
    {% set sql_parts = [] %}
    
    {% for t in table_names %}
        {% do sql_parts.append("""
            select 
                \"Month\", 
                \"Region\", 
                \"Market Channel\", 
                \"Job Name\", 
                \"Pending Backlog (Revenue)\", 
                \"Pending Backlog (Margin)\", '""" ~ t ~ "' as source_table from " ~ db_name ~ "." ~ schema_name ~ "." ~ t) %}
    {% endfor %}
    
    {{ return(sql_parts | join(" union all ")) }}
{% endmacro %}