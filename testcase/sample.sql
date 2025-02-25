WITH
  CTE1 AS (
    SELECT
      2 AS A
  ),
  CTE2 AS (
    SELECT
      5 AS a
  )
SELECT
  *
FROM
  CTE1
UNION
SELECT
  *
FROM
  CTE2
