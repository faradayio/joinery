-- pending: sqlite3 EXCEPT requires a type system
-- pending: trino EXCEPT requires a type system
--
-- EXCEPT

CREATE TEMP TABLE t1 (a INT64, b INT64);
INSERT INTO t1 VALUES (1, 2), (3, 4);

CREATE OR REPLACE TABLE __result1 AS
SELECT * EXCEPT (b) FROM t1;

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
);
INSERT INTO __expected1 VALUES
  (1),
  (3);

