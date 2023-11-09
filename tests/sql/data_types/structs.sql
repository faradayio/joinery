-- pending: snowflake Need to emulate using OBJECT. May be challenging.
-- pending: sqlite3 Need to build structs from scratch

CREATE OR REPLACE TABLE __result1 AS
WITH t AS (SELECT 1 AS a)
SELECT
    -- Not allowed on Trino.
    -- STRUCT() AS empty_struct,
    STRUCT(1) AS anon_value,
    STRUCT(1 AS a) AS named_value,
    STRUCT(a) AS inferred_name,
    STRUCT(1 AS a, 2 AS b) AS named_values,
    STRUCT<INT64>(NULL) AS anon_value_with_type,
    STRUCT<a INT64, b INT64>(1, 2) AS named_values_with_type,
    STRUCT([1] AS arr) AS struct_with_array,
    STRUCT(STRUCT(1 AS a) AS `inner`) AS struct_with_struct,
    --STRUCT(1 AS a).a AS struct_field_access,
FROM t;

CREATE OR REPLACE TABLE __expected1 (
    -- empty_struct STRUCT<>,
    anon_value STRUCT<INT64>,
    named_value STRUCT<a INT64>,
    inferred_name STRUCT<a INT64>,
    named_values STRUCT<a INT64, b INT64>,
    anon_value_with_type STRUCT<INT64>,
    named_values_with_type STRUCT<a INT64, b INT64>,
    struct_with_array STRUCT<arr ARRAY<INT64>>,
    struct_with_struct STRUCT<`inner` STRUCT<a INT64>>,
    --struct_field_access INT64,
);
INSERT INTO __expected1
SELECT
    -- STRUCT(), -- empty_struct
    STRUCT(1), -- anon_value
    STRUCT(1), -- named_value
    STRUCT(1), -- inferred_name
    STRUCT(1, 2), -- named_values
    STRUCT<INT64>(NULL), -- anon_value_with_type
    STRUCT(1, 2), -- named_values_with_type
    STRUCT([1]), -- struct_with_array
    STRUCT(STRUCT(1)); -- struct_with_struct
    --1; -- struct_field_access
