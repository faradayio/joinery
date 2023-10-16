-- pending: sqlite3 BigQuery RAND returns [0.0, 1.0), SQLite3 RANDOM() returns int

CREATE OR REPLACE TABLE __result1 AS
SELECT
    RAND() BETWEEN 0.0 AND 1.0 AS rand_in_range;

CREATE OR REPLACE TABLE __expected1 (
    rand_in_range BOOL,
);
INSERT INTO __expected1 VALUES
  (TRUE);
