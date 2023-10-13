-- Test type parsing without creating any actual rows. This allows us to
-- verify as many types as possible without needing to worry about reading
-- data from the database.
--
-- We have no `__result1` or `__expected1` tables because we have no data
-- to compare.

CREATE OR REPLACE TEMP TABLE column_types (
    null_bool BOOL,
    null_boolean BOOLEAN,
    null_int64 INT64,
    null_float64 FLOAT64,
    null_numeric NUMERIC,
    -- The only reason to use this is to never lose data, but there is no
    -- equivalent type in most databases.
    --
    -- null_bignumeric BIGNUMERIC,
    null_string STRING,
    null_bytes BYTES,
    null_date DATE,
    null_datetime DATETIME,
    null_time TIME,
    null_timestamp TIMESTAMP,
    null_geography GEOGRAPHY,
    null_array_bool ARRAY<BOOL>,
    null_array_int64 ARRAY<INT64>,
    null_struct_with_named_fields STRUCT<foo INT64, bar STRING>,
    null_struct_with_unnamed_fields STRUCT<INT64, STRING>,
);
