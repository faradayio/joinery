-- COALESCE

CREATE OR REPLACE TABLE __result1 AS
SELECT
    -- This is not supported on BigQuery.
    --
    -- COALESCE() AS coalesce_empty,
    --
    -- This is supported by BigQuery, but not by other databases. We can remove
    -- it in the transpiler if absolutely necessary, but it's probably better to
    -- make it an error.
    --
    -- COALESCE(1) AS coalesce_one,
    COALESCE(1, 2) AS coalesce_two,
    COALESCE(NULL, 2) AS coalesce_two_null,
    COALESCE(NULL, 2, 3) AS coalesce_three_null;

CREATE OR REPLACE TABLE __expected1 (
    coalesce_two INT64,
    coalesce_two_null INT64,
    coalesce_three_null INT64,
);
INSERT INTO __expected1 VALUES
  (1, 2, 2);
