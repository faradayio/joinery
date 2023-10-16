-- pending: sqlite3 GREATEST, LEAST are not available
--
-- GREATEST, LEAST

CREATE OR REPLACE TABLE __result1 AS
SELECT
    GREATEST(1, 2, 3) AS greatest,
    GREATEST(1, NULL, 3) AS greatest_null,
    LEAST(1, 2, 3) AS least,
    LEAST(1, NULL, 3) AS least_null;

CREATE OR REPLACE TABLE __expected1 (
    greatest INT64,
    greatest_null INT64,
    least INT64,
    least_null INT64,
);
INSERT INTO __expected1 VALUES
  (3, NULL, 1, NULL);
