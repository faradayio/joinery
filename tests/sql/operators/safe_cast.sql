-- pending: sqlite3 Returns 0, etc., on failure, instead of NULL
-- 
-- SAFE_CAST. For now, we only try to support string inputs.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    SAFE_CAST('1' AS INT64) AS cast_valid,
    SAFE_CAST('abc' AS INT64) AS cast_invalid,
    SAFE_CAST(NULL AS INT64) AS cast_null;

CREATE OR REPLACE TABLE __expected1 (
    cast_valid INT64,
    cast_invalid INT64,
    cast_null INT64,
);
INSERT INTO __expected1 VALUES
  (1, NULL, NULL);
