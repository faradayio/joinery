CREATE TEMP TABLE table_star (a INT64);
INSERT INTO table_star VALUES (1), (2);

CREATE OR REPLACE TABLE __result1 AS
SELECT t.* FROM table_star AS t;

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
);
INSERT INTO __expected1 VALUES
  (1),
  (2);
