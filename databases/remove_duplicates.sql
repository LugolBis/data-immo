with mutations_duplicate as (
    SELECT
        idg,
        ROWNUMBER() OVER (PARTITION BY idg, idpar, datemut ORDER BY idg) as rn
    FROM
        Mutations
),
to_delete AS (
    SELECT
        idg
    FROM
        mutations_duplicate
    WHERE
        rn > 1
)

DELETE FROM Mutations 
WHERE idg IN (SELECT idg FROM to_delete);