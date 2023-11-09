-- pending: sqlite3 Not yet implemented
--
-- Basic math functions.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    MOD(5, 2) AS mod,
    MOD(-5, 2) AS mod_neg;

CREATE OR REPLACE TABLE __expected1 (
    mod INT64,
    mod_neg INT64,
);
INSERT INTO __expected1 VALUES
  (1, -1);
