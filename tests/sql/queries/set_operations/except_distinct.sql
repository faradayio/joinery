-- INTERSECT DISTINCT
CREATE OR REPLACE TABLE __result1 AS
(SELECT 1 AS i UNION ALL SELECT 1 UNION ALL SELECT 2)
EXCEPT DISTINCT
(SELECT 1);

CREATE OR REPLACE TABLE __expected1 (
    i INT64,
);
INSERT INTO __expected1 VALUES
  (2);
