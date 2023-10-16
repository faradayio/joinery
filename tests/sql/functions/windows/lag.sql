-- LAG

CREATE TEMP TABLE fruits (idx INT64, fruit_name STRING);
INSERT INTO fruits VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry');

CREATE OR REPLACE TABLE __result1 AS
SELECT
    fruit_name,
    LAG(fruit_name) OVER (ORDER BY idx) AS previous_fruit
FROM fruits;

CREATE OR REPLACE TABLE __expected1 (
    fruit_name STRING,
    previous_fruit STRING,
);
INSERT INTO __expected1 VALUES
  ('apple', NULL),
  ('banana', 'apple'),
  ('cherry', 'banana');
