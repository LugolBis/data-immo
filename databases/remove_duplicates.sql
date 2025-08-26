CREATE TEMPORARY TABLE mutations_to_delete AS
WITH mutations_duplicate AS (
    SELECT
        idg,
        row_number() OVER (PARTITION BY idg, idpar, datemut ORDER BY idg) as rn
    FROM
        Mutations
)
SELECT
    idg
FROM
    mutations_duplicate
WHERE
    rn > 1;

DELETE FROM Mutations 
WHERE idg IN (SELECT idg FROM mutations_to_delete);

DELETE FROM Classes
WHERE idg IN (SELECT idg from mutations_to_delete);

DROP TABLE mutations_to_delete;