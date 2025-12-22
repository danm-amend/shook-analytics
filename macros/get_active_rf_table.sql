-- deprecated
{% macro get_active_rf_table() %}
  {% set sql %}
    select table_name
    from shookdw.metadata.rolling_forecast_file_control
    where active = true
    limit 1
  {% endset %}

  {% if execute %}
    {% set results = run_query(sql) %}
    {% if results|length > 0 %}
      {% set table_name = results.columns[0].values()[0] %}
      {{ return('shookdw' ~ "." ~ 'sharepoint' ~ "." ~ table_name) }}
    {% else %}
      {{ exceptions.raise_compiler_error("No active table found in control table") }}
    {% endif %}
  {% else %}
    {{ return("shookdw.sharepoint.rolling_forecast_jun_2025") }}
  {% endif %}
{% endmacro %}
