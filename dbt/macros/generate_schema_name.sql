{% macro generate_schema_name(custom_schema_name, node) %}
    {% set default_schema = target.schema %}

    {% if custom_schema_name is none %}
        {% set path_parts = node.path.split('/') %}

        {% if path_parts[0] == 'staging' %}
            {% set folder = path_parts[1] %}
            {{ "stg_" ~ folder }}

        {% elif path_parts[0] == 'intermediate' %}
            {{ "int" }}

        {% elif path_parts[0] == 'marts' %}
            {% set folder = path_parts[1] %}
            {{ "marts_" ~ folder }}

        {% else %}
            {{ default_schema }}
        {% endif %}

    {% else %}
        {{ custom_schema_name }}
    {% endif %}
{% endmacro %}
