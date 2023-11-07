-- QUALIFY has a portabe rewrite using a nested query, so we expect it to work
-- everywhere.

CREATE TEMP TABLE stores (id INT64, state STRING, sales INT64);
INSERT INTO stores VALUES
  (1, 'CA', 100),
  (2, 'CA', 200),
  (3, 'NY', 300),
  (4, 'NY', 400),
  (5, 'NY', 500);

-- Compute top sellers in each state.
CREATE OR REPLACE TABLE __result1 AS
SELECT id, state, sales
FROM stores
QUALIFY RANK() OVER (PARTITION BY state ORDER BY sales DESC) = 1;

CREATE OR REPLACE TABLE __expected1 (
    id INT64,
    state STRING,
    sales INT64,
);
INSERT INTO __expected1 VALUES
  (5, 'NY', 500),
  (2, 'CA', 200);
