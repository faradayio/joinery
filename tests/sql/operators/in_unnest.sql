-- pending: snowflake Know how we could transpile, see source.
-- pending: sqlite3 No array support.
--
-- UNNEST
--
-- Here's how to get close.
--
-- ```sql
-- CREATE TEMP FUNCTION unnest(arr ARRAY) RETURNS TABLE(val VARIANT)
-- AS $$ SELECT value FROM TABLE(FLATTEN(INPUT => arr)) $$;
--
-- SELECT 2 IN (SELECT * FROM TABLE(UNNEST([1,2,3])))
-- ```

CREATE OR REPLACE TABLE __result1 AS
SELECT
    1 IN UNNEST([1, 2, 3]) AS in1,
    1 IN UNNEST([2, 3]) AS in2;

CREATE OR REPLACE TABLE __expected1 (
    in1 BOOL,
    in2 BOOL,
);
INSERT INTO __expected1 VALUES
  (TRUE, FALSE);
