-- pending: trino BigQuery NUMERIC may lose data when converted to DECIMAL(n,m).

-- Test type parsing without creating any actual rows. This allows us to
-- verify as many types as possible without needing to worry about reading
-- data from the database.
--
-- We have no `__result1` or `__expected1` tables because we have no data
-- to compare.

CREATE OR REPLACE TEMP TABLE column_types (
    null_numeric NUMERIC,
);
