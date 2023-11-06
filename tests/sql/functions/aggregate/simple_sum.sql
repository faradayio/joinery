-- A simple GROUP BY query with SUM() aggregate function.

CREATE TEMP TABLE ints_for_sum (grp STRING, i INT64);
INSERT INTO ints_for_sum VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4);

CREATE OR REPLACE TABLE __result1 AS
SELECT grp, SUM(i) AS sum
FROM ints_for_sum
GROUP BY grp;

CREATE OR REPLACE TABLE __expected1 (
    grp STRING,
    sum INT64,
);
INSERT INTO __expected1 VALUES
  ('a', 3),
  ('b', 7);
