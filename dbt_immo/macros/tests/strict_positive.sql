{% test strict_positive(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 1.0

{% endtest %}