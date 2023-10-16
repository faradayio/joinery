-- pending: snowflake ARRAY(SELECT ...) requires significant transformation
-- pending: sqlite3 Does not support arrays
--
-- ARRAY(SELECT ...)

CREATE TEMP TABLE array_items (idx INT64, val INT64);
INSERT INTO array_items VALUES (1, 1), (2, 3);

CREATE OR REPLACE TABLE __result1 AS
SELECT ARRAY(SELECT val FROM array_items ORDER BY idx) AS arr;

CREATE OR REPLACE TABLE __expected1 (
    arr ARRAY<INT64>
);
-- Snowflake does not allow array constants in VALUES.
INSERT INTO __expected1
SELECT [1, 3];
