-- pending: sqlite3 Does not support arrays
--
-- ARRAY(SELECT ...)
--
-- See https://github.com/trinodb/trino/issues/12

CREATE OR REPLACE TABLE __result1 AS
SELECT ARRAY(
    SELECT DISTINCT val*2 FROM UNNEST([1,3,3]) AS val
) AS arr;

CREATE OR REPLACE TABLE __expected1 (
    arr ARRAY<INT64>
);
-- Snowflake does not allow array constants in VALUES.
INSERT INTO __expected1
SELECT [2, 6];

-- Now let's do it again, but this time with a correlated subquery. This tends
-- to break simple transformation strategies.

CREATE TEMP TABLE array_select_data (
    arr ARRAY<INT64>
);
INSERT INTO array_select_data VALUES
    ([1, 2, 3]),
    ([2, 4, 4]);

CREATE OR REPLACE TABLE __result2 AS
SELECT ARRAY(
    SELECT DISTINCT val / 2 FROM UNNEST(arr) AS val WHERE MOD(val, 2) = 0
) AS arr
FROM array_select_data;

CREATE OR REPLACE TABLE __expected2 (
    arr ARRAY<INT64>
);
INSERT INTO __expected2
SELECT [1]
UNION ALL
SELECT [1, 2];
