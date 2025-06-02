{% macro generate_schema_name(custom_schema_name, node) %}
    {% set env = env_var('DBT_CLOUD_ENVIRONMENT_NAME', 'dev') %}
    {% if env == 'prod'%}
        {{custom_schema_name}}
    {% else %}
        {{ target.schema }}_{{ custom_schema_name  | trim}}
    {% endif %}
{% endmacro %}


