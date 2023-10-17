-- HAVING

CREATE TEMP TABLE having_data (grp STRING, val INT64);
INSERT INTO having_data VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4);

CREATE OR REPLACE TABLE __result1 AS
SELECT grp, SUM(val) AS sum_val
FROM having_data
GROUP BY grp
HAVING sum_val > 3;

CREATE OR REPLACE TABLE __expected1 (
    grp STRING,
    sum_val INT64,
);
INSERT INTO __expected1 VALUES
  ('b', 7);
