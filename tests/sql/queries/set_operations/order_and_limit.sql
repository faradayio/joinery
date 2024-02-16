-- ORDER BY and LIMIT can be applied to set operations.

CREATE OR REPLACE TABLE __result1 AS
(
    SELECT 1 AS i
    UNION ALL
    SELECT 3
    UNION ALL
    SELECT 2
)
ORDER BY i ASC
LIMIT 2;

CREATE OR REPLACE TABLE __expected1 (
    i INT64,
);
INSERT INTO __expected1 VALUES
  (1),
  (2);

-- Now let's try hard mode. This actually works in BigQuery.and Trino,
-- unchanged.
CREATE OR REPLACE TABLE __result2 AS
WITH names1 AS (
    SELECT 1 AS id, 'a' AS name
),
names2 AS (
    SELECT 2 AS id, 'c' AS name
),
streets AS (
    SELECT 1 AS id, 'b' AS street
    UNION ALL
    SELECT 2 AS id, 'd' AS street
)

(
    (
      SELECT * FROM names1 AS n
      JOIN streets USING (id)
      -- Can use streets.street here.
      ORDER BY streets.street
    )

    UNION ALL

    (
        (
            SELECT * FROM names2 AS n
            JOIN streets USING (id)
        )
        -- Cannot use streets.street here.
        ORDER BY street
    )
)
-- Cannot use streets.street here.
ORDER BY name, street;

CREATE OR REPLACE TABLE __expected2 (
    id INT64,
    name STRING,
    street STRING
);
INSERT INTO __expected2 VALUES
  (1, 'a', 'b'),
  (2, 'c', 'd');
