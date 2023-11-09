-- pending: sqlite3 Needs external function.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    LENGTH(generate_uuid()) AS uuid_length;

CREATE OR REPLACE TABLE __expected1 (
    uuid_length INT64,
);
INSERT INTO __expected1 VALUES
  (36);
