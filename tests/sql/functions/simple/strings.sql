-- LOWER, UPPER, TRIM, CONCAT
CREATE OR REPLACE TABLE __result1 AS
SELECT
    LOWER('FOO') AS lower,
    UPPER('foo') AS upper,
    TRIM('  foo  ') AS trim,
    CONCAT('foo', 'bar') AS concat,
    CONCAT('x', 1) concat_casted,
    CONCAT('x', NULL) concat_null;

CREATE OR REPLACE TABLE __expected1 (
    lower STRING,
    upper STRING,
    trim STRING,
    concat STRING,
    concat_casted STRING,
    concat_null STRING,
);
INSERT INTO __expected1 VALUES
  ('foo', 'FOO', 'foo', 'foobar', 'x1', NULL);

