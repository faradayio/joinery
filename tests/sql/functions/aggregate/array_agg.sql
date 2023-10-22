-- pending: sqlite3 Arrays are not available.

CREATE TEMP TABLE array_agg_data (grp STRING, idx INT64, x INT64);
INSERT INTO array_agg_data
VALUES ('a', 1, 1), ('a', 2, 1), ('a', 3, 2), ('a', 4, 0);

CREATE OR REPLACE TABLE __result1 AS
SELECT grp, ARRAY_AGG(x) AS arr
FROM (
  SELECT grp, x
  FROM array_agg_data
  ORDER BY idx
)
GROUP BY grp;

CREATE OR REPLACE TABLE __expected1 (
    grp STRING,
    arr ARRAY<INT64>,
);
-- Snowflake does not allow array constants in VALUES.
INSERT INTO __expected1
SELECT 'a', [1, 1, 2, 0];

-- Now test DISTINCT.
CREATE OR REPLACE TABLE __result2 AS
SELECT grp, ARRAY_AGG(DISTINCT x ORDER BY x) AS arr
FROM array_agg_data
GROUP BY grp;

CREATE OR REPLACE TABLE __expected2 (
    grp STRING,
    arr ARRAY<INT64>,
);
-- Snowflake does not allow array constants in VALUES.
INSERT INTO __expected2
SELECT 'a', [0, 1, 2];