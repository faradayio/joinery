-- INSERT DISTINCT
CREATE OR REPLACE TABLE __result1 AS
WITH t1 AS (SELECT 1 AS i UNION ALL SELECT 2),
     t2 AS (SELECT 2 AS i UNION ALL SELECT 3)
SELECT * FROM t1
INTERSECT DISTINCT
SELECT * FROM t2;

CREATE OR REPLACE TABLE __expected1 (
    i INT64,
);
INSERT INTO __expected1 VALUES
  (2);

-- Now inline it, because we need make sure we handle parens correctly.
CREATE OR REPLACE TABLE __result2 AS
(SELECT 1 AS i UNION ALL SELECT 2)
INTERSECT DISTINCT
(SELECT 2 AS i UNION ALL SELECT 3);

CREATE OR REPLACE TABLE __expected2 AS SELECT * FROM __expected1;
