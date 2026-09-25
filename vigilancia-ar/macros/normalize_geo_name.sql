{% macro normalize_geo_name(column) %}
    trim(regexp_replace(
        regexp_replace(
            upper(translate({{ column }}, 'áéíóúñüÁÉÍÓÚÑÜ', 'aeiounuAEIOUNU')),
            '\\b(GRL|GRAL)\\.?\\b', 'GENERAL'
        ),
        '\\s+', ' '
    ))
{% endmacro %}
