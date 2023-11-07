-- Implicit aggregates.

CREATE TEMP TABLE implicit_agg (val INT64);
INSERT INTO implicit_agg VALUES (1), (2), (3);

CREATE OR REPLACE TABLE __result1 AS
SELECT SUM(val) AS sum_val
FROM implicit_agg;

CREATE OR REPLACE TABLE __expected1 (
    sum_val INT64,
);
INSERT INTO __expected1 VALUES
  (6);

-- Now make sure we _don't_ trigger if the aggregate appears in a subquery.
CREATE OR REPLACE TABLE __result2 AS
SELECT x, (SELECT SUM(val) FROM implicit_agg) AS sum_val
FROM (SELECT 1 AS x);

CREATE OR REPLACE TABLE __expected2 (
    x INT64,
    sum_val INT64,
);
INSERT INTO __expected2 VALUES
  (1, 6);
