-- pending: sqlite3 No array support.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    [] AS empty_array,
    [1, 2, 3] AS int64_array,
    ['a', 'b', 'c'] AS string_array,
    ARRAY<INT64>[] AS empty_array_typed,
    ARRAY<INT64>[1, 2, 3] AS int64_array_typed,
    ARRAY(1, 2, 3) AS int64_array_typed2,
    ARRAY_LENGTH([1,2]) AS array_len;

CREATE OR REPLACE TABLE __expected1 (
    empty_array ARRAY<INT64>,
    int64_array ARRAY<INT64>,
    string_array ARRAY<STRING>,
    empty_array_typed ARRAY<INT64>,
    int64_array_typed ARRAY<INT64>,
    int64_array_typed2 ARRAY<INT64>,
    array_len INT64,
);
-- Snowflake does not allow array constants in VALUES.
INSERT INTO __expected1
SELECT [], [1, 2, 3], ['a', 'b', 'c'], [], [1, 2, 3], [1, 2, 3], 2;
