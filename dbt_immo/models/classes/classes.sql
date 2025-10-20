WITH filled_values AS (
    SELECT
        m.idg as idg,
        COALESCE(c.libelle, 'Unknown') AS libelle,
        COALESCE(c.surface, 1.0) AS surface
    FROM
        {{ ref("mutations") }} AS m
        LEFT OUTER JOIN DVF."classes.parquet" AS c
        ON m.idg = c.idg
)

SELECT *
FROM filled_values