{% macro generate_schema_name(custom_schema_name, node) %}
  {% if env_var('DBT_IS_PROD', 'false') == 'true' %}
    profitability
  {% else %}
    {{ target.schema }}_{{ custom_schema_name if custom_schema_name else node.name }}
  {% endif %}
{% endmacro %}
