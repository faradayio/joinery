-- pending: sqlite3 Not yet implemented
--
-- Basic math functions.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    -(2) AS neg,
    MOD(5, 2) AS mod,
    MOD(-5, 2) AS mod_neg;

CREATE OR REPLACE TABLE __expected1 (
    neg INT64,
    mod INT64,
    mod_neg INT64,
);
INSERT INTO __expected1 VALUES
  (-2, 1, -1);
