-- CAST(NULL AS <type>) for common types.
CREATE TABLE __result1 AS
SELECT
    CAST(NULL AS BOOL) AS null_bool,
    CAST(NULL AS BOOLEAN) AS null_boolean,
    CAST(NULL AS INT64) AS null_int64,
    CAST(NULL AS FLOAT64) AS null_float64,
    -- CAST(NULL AS NUMERIC) AS null_numeric,
    -- CAST(NULL AS BIGNUMERIC) AS null_bignumeric,
    CAST(NULL AS STRING) AS null_string,
    -- CAST(NULL AS BYTES) AS null_bytes,
    -- CAST(NULL AS DATE) AS null_date,
    CAST(NULL AS DATETIME) AS null_datetime,
    -- CAST(NULL AS TIME) AS null_time,
    -- CAST(NULL AS TIMESTAMP) AS null_timestamp,
    -- CAST(NULL AS GEOGRAPHY) AS null_geography,
    CAST(NULL AS ARRAY<BOOL>) AS null_array_bool,
    CAST(NULL AS ARRAY<INT64>) AS null_array_int64,
    CAST(NULL AS STRUCT<foo INT64, bar STRING>) AS null_struct_with_named_fields,
    CAST(NULL AS STRUCT<INT64, STRING>) AS null_struct_with_unnamed_fields;

CREATE TABLE __expected1 (
    null_bool BOOL,
    null_boolean BOOLEAN,
    null_int64 INT64,
    null_float64 FLOAT64,
    -- null_numeric NUMERIC,
    -- null_bignumeric BIGNUMERIC,
    null_string STRING,
    -- null_bytes BYTES,
    -- null_date DATE,
    null_datetime DATETIME,
    -- null_time TIME,
    -- null_timestamp TIMESTAMP,
    -- null_geography GEOGRAPHY,
    null_array_bool ARRAY<BOOL>,
    null_array_int64 ARRAY<INT64>,
    null_struct_with_named_fields STRUCT<foo INT64, bar STRING>,
    null_struct_with_unnamed_fields STRUCT<INT64, STRING>,
);
INSERT INTO __expected1 VALUES (
    NULL, -- null_bool
    NULL, -- null_boolean
    NULL, -- null_int64
    NULL, -- null_float64
    -- NULL, -- null_numeric
    -- NULL, -- null_bignumeric
    NULL, -- null_string
    -- NULL, -- null_bytes
    -- NULL, -- null_date
    NULL, -- null_datetime
    -- NULL, -- null_time
    -- NULL, -- null_timestamp
    -- NULL, -- null_geography
    NULL, -- null_array_bool
    NULL, -- null_array_int64
    NULL, -- null_struct_with_named_fields
    NULL, -- null_struct_with_unnamed_fields
);