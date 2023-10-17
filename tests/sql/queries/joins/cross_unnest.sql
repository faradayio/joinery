-- pending: snowflake Should be transpilable.
-- pending: sqlite3 No array support.
--
-- CROSS JOIN UNNEST

CREATE TEMP TABLE t1 (a INT64, b ARRAY<STRING>);
-- Array literals don't work in Snowflake.
INSERT INTO t1
SELECT 1, ['s', 't']
UNION ALL
SELECT 2, ['u', 'v'];

CREATE OR REPLACE TABLE __result1 AS
SELECT a, b_val
FROM t1
CROSS JOIN UNNEST(b) AS b_val;

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
    b_val STRING,
);
INSERT INTO __expected1 VALUES
  (1, 's'),
  (1, 't'),
  (2, 'u'),
  (2, 'v');
