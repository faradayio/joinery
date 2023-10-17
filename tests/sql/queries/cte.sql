-- WITH

CREATE OR REPLACE TABLE __result1 AS
WITH t1 AS (
  SELECT 1 AS a, 's' AS b UNION ALL
  SELECT 2 AS a, 't' AS b
),
t2 AS (
  SELECT 1 AS a, 'u' AS c UNION ALL
  SELECT 3 AS a, 'v' AS c
)
SELECT t1.a, t1.b, t2.c
FROM t1
INNER JOIN t2 USING (a);

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
    b STRING,
    c STRING,
);
INSERT INTO __expected1 VALUES
  (1, 's', 'u');
