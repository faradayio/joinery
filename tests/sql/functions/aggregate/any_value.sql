-- pending: sqlite3 ANY_VALUE is not available.

CREATE TEMP TABLE vals (i INT64);
INSERT INTO vals VALUES (1), (1);

CREATE OR REPLACE TABLE __result1 AS
SELECT ANY_VALUE(i) AS any_value FROM vals;

CREATE OR REPLACE TABLE __expected1 (
    any_value INT64,
);
INSERT INTO __expected1 VALUES
  (1);