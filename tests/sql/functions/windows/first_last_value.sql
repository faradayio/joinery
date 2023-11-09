-- FIRST_VALUE, LAST_VALUE

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

CREATE OR REPLACE TABLE __result1 AS
SELECT
    item,
    FIRST_VALUE(item) OVER (PARTITION BY category ORDER BY price) AS cheapest_alternative,
    -- We need the RANGE frame here.
    LAST_VALUE(item) OVER (PARTITION BY category ORDER BY price RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS most_expensive_alternative,
FROM groceries;

CREATE OR REPLACE TABLE __expected1 (
    item STRING,
    cheapest_alternative STRING,
    most_expensive_alternative STRING,
);
INSERT INTO __expected1 VALUES
  ('apple', 'apple', 'banana'),
  ('banana', 'apple', 'banana'),
  ('carrot', 'carrot', 'eggplant'),
  ('eggplant', 'carrot', 'eggplant'),
  ('flour', 'flour', 'salt'),
  ('sugar', 'flour', 'salt'),
  ('salt', 'flour', 'salt');
