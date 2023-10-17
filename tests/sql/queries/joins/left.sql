-- LEFT JOIN

CREATE TEMP TABLE t1 (a INT64, b STRING);
INSERT INTO t1 VALUES (1, 's'), (2, 't');

CREATE TEMP TABLE t2 (a INT64, c STRING);
INSERT INTO t2 VALUES (1, 'u'), (3, 'v');

CREATE OR REPLACE TABLE __result1 AS
SELECT t1.a, t1.b, t2.c
FROM t1
LEFT JOIN t2 ON t1.a = t2.a;

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
    b STRING,
    c STRING,
);
INSERT INTO __expected1 VALUES
  (1, 's', 'u'),
  (2, 't', NULL);
