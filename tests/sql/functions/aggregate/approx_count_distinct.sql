-- pending: sqlite3 No APPROX_COUNT_DISTINCT function.

CREATE TEMP TABLE approx_count_distinct_data (x INT64);
INSERT INTO approx_count_distinct_data VALUES (1), (2), (2), (2), (3);

CREATE OR REPLACE TABLE __result1 AS
SELECT APPROX_COUNT_DISTINCT(x) AS approx_count_distinct
FROM approx_count_distinct_data;

CREATE OR REPLACE TABLE __expected1 (
    approx_count_distinct INT64,
);
INSERT INTO __expected1 VALUES
  (3);
