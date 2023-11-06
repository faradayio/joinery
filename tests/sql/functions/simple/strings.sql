-- LOWER, UPPER, TRIM, CONCAT
CREATE OR REPLACE TABLE __result1 AS
SELECT
    LOWER('FOO') AS lower,
    UPPER('foo') AS upper,
    TRIM('  foo  ') AS trimmed,
    CONCAT('foo', 'bar') AS concat,
    -- We have decided not to support this for now, because it requires casting
    -- all types _except_ BINARY.
    --
    -- CONCAT('x', 1) concat_casted,
    CONCAT('x', NULL) concat_null;

CREATE OR REPLACE TABLE __expected1 (
    lower STRING,
    upper STRING,
    trimmed STRING,
    concat STRING,
    concat_null STRING,
);
INSERT INTO __expected1 VALUES
  ('foo', 'FOO', 'foo', 'foobar', NULL);

