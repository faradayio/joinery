-- CROSS JOIN

CREATE TEMP TABLE cj_t1 (a INT64);
INSERT INTO cj_t1 VALUES (1), (2);

CREATE TEMP TABLE cj_t2 (b INT64);
INSERT INTO cj_t2 VALUES (3), (4);

CREATE OR REPLACE TABLE __result1 AS
SELECT cj_t1.a, cj_t2.b
FROM cj_t1
CROSS JOIN cj_t2;

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
    b INT64,
);
INSERT INTO __expected1 VALUES
  (1, 3),
  (1, 4),
  (2, 3),
  (2, 4);
