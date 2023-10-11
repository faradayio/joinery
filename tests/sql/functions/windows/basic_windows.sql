-- Test cases for basic window functions and syntax.
--
-- There are lots of good public domain tests at
-- https://www.sqlite.org/src/file?name=test/window1.test
-- that could be adapted for use here.

-- A fixture table for testing window functions.
CREATE TEMP TABLE groceries (
  item STRING,
  category STRING,
  price FLOAT64,
);
INSERT INTO groceries VALUES
    ('apple', 'fruit', 1.00),
    ('banana', 'fruit', 1.50),
    ('carrot', 'vegetable', 0.75),
    ('eggplant', 'vegetable', 1.25),
    ('sugar', 'baking', 0.50),
    ('flour', 'baking', 0.25),
    ('salt', 'baking', 0.75);

CREATE TABLE __result1 AS
SELECT
    item,
    category,
    price,
    RANK() OVER (PARTITION BY category ORDER BY price) AS price_rank,
    SUM(price) OVER (PARTITION BY category ORDER BY price ROWS UNBOUNDED PRECEDING) AS cummulative_price,
    SUM(price) OVER (PARTITION BY category ORDER BY price ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cummulative_price2,
FROM groceries;

CREATE TABLE __expected1 (
    item STRING,
    category STRING,
    price FLOAT64,
    price_rank INT64,
    cummulative_price FLOAT64,
    cummulative_price2 FLOAT64,
);
INSERT INTO __expected1 VALUES
  ('apple', 'fruit', 1.0, 1, 1.0, 1.0),
  ('banana', 'fruit', 1.5, 2, 2.5, 2.5),
  ('carrot', 'vegetable', 0.75, 1, 0.75, 0.75),
  ('eggplant', 'vegetable', 1.25, 2, 2.0, 2.0),
  ('flour', 'baking', 0.25, 1, 0.25, 0.25),
  ('sugar', 'baking', 0.5, 2, 0.75, 0.75),
  ('salt', 'baking', 0.75, 3, 1.5, 1.5);
