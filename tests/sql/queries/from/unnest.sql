-- pending: snowflake Shouldn't be hard.
-- pending: sqlite3 No array support.
--
-- FROM UNNEST

CREATE OR REPLACE TABLE __result1 AS
SELECT i FROM UNNEST([1, 2, 3]) AS i;

CREATE OR REPLACE TABLE __expected1 (
    i INT64,
);
INSERT INTO __expected1 VALUES
  (1),
  (2),
  (3);
