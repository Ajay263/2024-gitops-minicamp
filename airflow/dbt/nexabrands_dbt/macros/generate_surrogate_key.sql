-- macros/generate_surrogate_key.sql
{% macro generate_surrogate_key(field_list) %}
    {% if varargs|length >= 1 %}
        {{ exceptions.raise_compiler_error("generate_surrogate_key takes a single list argument, instead of multiple arguments") }}
    {% endif %}

    {%- set fields = [] -%}

    {%- for field in field_list -%}
        {%- set _ = fields.append(
            "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '')"
        ) -%}
    {%- endfor -%}

    {{ dbt.hash(dbt.concat(fields)) }}
{% endmacro %}
