-- pending: sqlite3 ARRAY not supported

CREATE OR REPLACE TABLE __result1 AS
SELECT
CASE
WHEN TRUE THEN ARRAY[1]
ELSE CAST(NULL AS ARRAY<INT64>)
END AS case_when_true;

CREATE OR REPLACE TABLE __expected1 (
    case_when_true ARRAY<INT64>,
);
-- Snowflake does not allow array constants in VALUES.
INSERT INTO __expected1
SELECT [1];