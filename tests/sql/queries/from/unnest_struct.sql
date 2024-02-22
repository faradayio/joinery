-- pending: snowflake Shouldn't be hard
-- pending: sqlite3 No array support
--
-- FROM UNNEST

CREATE OR REPLACE TABLE __result1 AS
SELECT * FROM UNNEST([STRUCT<a INT64, b INT64>(1, 2)]);

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
    b INT64,
);
INSERT INTO __expected1 VALUES
  (1, 2);
