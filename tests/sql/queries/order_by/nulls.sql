-- ORDER BY DESC NULLS LAST

CREATE TEMP TABLE order_by_data (val INT64);
INSERT INTO order_by_data VALUES (1), (2), (NULL), (3), (NULL);

CREATE OR REPLACE TABLE __result1 AS
SELECT val
FROM order_by_data
ORDER BY val DESC NULLS LAST
LIMIT 4;

-- Note that we do not check the order of this table, merely what rows it
-- contains.
CREATE OR REPLACE TABLE __expected1 (
    val INT64,
);
INSERT INTO __expected1 VALUES
  (3),
  (2),
  (1),
  (NULL);
