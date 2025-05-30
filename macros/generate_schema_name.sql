{% macro generate_schema_name(custom_schema_name, node) %}
  {% set is_prod = env_var('DBT_ENV_NAME', 'dev') | lower == 'prod' %}
  
  {% if is_prod %}
    {{ custom_schema_name }}
  {% else %}
    {{ target.schema }}_{{ custom_schema_name }}
  {% endif %}
{% endmacro %}
