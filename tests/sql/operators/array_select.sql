-- pending: snowflake ARRAY(SELECT ...) requires significant transformation
-- pending: sqlite3 Does not support arrays
--
-- ARRAY(SELECT ...)
--
-- See https://github.com/trinodb/trino/issues/12

CREATE OR REPLACE TABLE __result1 AS
SELECT ARRAY(
    -- Using SELECT DISTINCT may change the order of the elements.
    SELECT val*2 AS val_dbl FROM UNNEST([1,3,3]) AS val
) AS arr;

CREATE OR REPLACE TABLE __expected1 (
    arr ARRAY<INT64>
);
-- Snowflake does not allow array constants in VALUES.
INSERT INTO __expected1
SELECT [2, 6, 6];
