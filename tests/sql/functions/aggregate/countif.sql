-- pending: snowflake COUNTIF can be rewritten portably by transpiler
-- pending: sqlite3 COUNTIF Can be rewritten portably by transpiler

CREATE TEMP TABLE vals (i INT64);
INSERT INTO vals VALUES (1), (2), (2), (3), (NULL);

CREATE OR REPLACE TABLE __result1 AS
SELECT COUNTIF(i = 2) AS is2 FROM vals;

CREATE OR REPLACE TABLE __expected1 (
    is2 INT64,
);
INSERT INTO __expected1 VALUES
  (2);
