{% test text_length(model, column_name) %}

{% set parts = column_name.split('&') %}
{% set column_name_real = parts[0] %}
{% set len_val = parts[1] %}

SELECT *
FROM {{ model }}
WHERE {{ column_name_real }} IS NOT NULL
    AND LENGTH({{ column_name_real }}) != {{ len_val }}

{% endtest %}