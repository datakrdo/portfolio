{% macro normalize_age_desc(column) %}
    lower(trim(regexp_replace(
        translate({{ column }}, 'áéíóúñÁÉÍÓÚÑ', 'aeiounAEIOUN'),
        '\\s+', ' '
    )))
{% endmacro %}
