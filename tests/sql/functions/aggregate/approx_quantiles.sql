-- pending: snowflake Use APPROX_PERCENTILE instead of APPROX_QUANTILES (complicated)
-- pending: sqlite3 No APPROX_QUANTILES function
-- pending: trino Use APPROX_PERCENTILE instead of APPROX_QUANTILES (complicated)

CREATE TEMP TABLE quantile_data (x INT64);
INSERT INTO quantile_data VALUES (1), (2), (3), (4), (5);

CREATE OR REPLACE TABLE __result1 AS
SELECT APPROX_QUANTILES(x, 2) AS approx_quantiles
FROM quantile_data;

CREATE OR REPLACE TABLE __expected1 (
    approx_quantiles ARRAY<INT64>,
);
INSERT INTO __expected1 VALUES
  ([1, 3, 5]);
