-- pending: sqlite3 No regex fuctions
--
-- REGEXP_REPLACE, REGEXP_EXTRACT
--
-- https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
--
-- We should consider testing a larger set of regular expression features,
-- because different databases may support different regex syntax.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    REGEXP_REPLACE('foo', r'oo', 'ee') AS replaced,
    REGEXP_EXTRACT('foobar', r'o+') AS extracted;

CREATE OR REPLACE TABLE __expected1 (
    replaced STRING,
    extracted STRING,
);
INSERT INTO __expected1 VALUES ('fee', 'oo');
