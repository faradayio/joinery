-- pending: sqlite3 EXP is not available

CREATE OR REPLACE TABLE __result1 AS
SELECT
    EXP(0.0) AS exp_zero;

CREATE OR REPLACE TABLE __expected1 (
    exp_zero FLOAT64,
);
INSERT INTO __expected1 VALUES
  (1.0);
