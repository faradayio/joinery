-- pending: snowflake Need to force AVG(INT64|FLOAT64) to return FLOAT64, not DECIMAL
--
-- ANSI standard aggregate functions: AVG, COUNT, MAX, MIN, SUM

CREATE TEMP TABLE ints (i INT64);
INSERT INTO ints VALUES (1), (2), (2), (3);

CREATE OR REPLACE TABLE __result1 AS
SELECT
    AVG(i) AS avg,
    COUNT(i) AS count_all,
    COUNT(DISTINCT i) AS count_distinct,
    MAX(i) AS max,
    MIN(i) AS min,
    SUM(i) AS sum
FROM ints;

CREATE OR REPLACE TABLE __expected1 (
    avg FLOAT64,
    count_all INT64,
    count_distinct INT64,
    max INT64,
    min INT64,
    sum INT64,
);

INSERT INTO __expected1 VALUES
  (2.0, 4, 3, 3, 1, 8);
