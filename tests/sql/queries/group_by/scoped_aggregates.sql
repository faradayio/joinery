-- In the example below, `t1.g` has a regular value type, and `t1.x` has an
-- aggregate type. Verified on BigQuery, Trino, Snowflake and SQLite3. This
-- is a real thing!

CREATE OR REPLACE TABLE __result1 AS
WITH t1 AS (SELECT 'a' AS g, 1 AS x)
SELECT t1.g, SUM(t1.x) AS x_sum
FROM t1
GROUP BY t1.g;

CREATE OR REPLACE TABLE __expected1 (
    g STRING,
    x_sum INT64,
);
INSERT INTO __expected1 VALUES
  ('a', 1);
