-- SUM(SUM(x)) is a thing. This affects the design of the type system.

create temp table nested_sum_t1 (
  g1 STRING,
  g2 STRING,
  x INT64
);

insert into nested_sum_t1 values
  ('a', 'x', 1),
  ('a', 'y', 2),
  ('b', 'x', 3),
  ('b', 'y', 4);

CREATE OR REPLACE TABLE __result1 AS
SELECT g1, g2, SUM(SUM(x)) OVER (PARTITION BY g2) AS `sum`
FROM nested_sum_t1 
GROUP BY g1, g2;

CREATE OR REPLACE TABLE __expected1 (
    g1 STRING,
    g2 STRING,
    `sum` INT64
);
INSERT INTO __expected1 VALUES
    ('a', 'x', 4),
    ('a', 'y', 6),
    ('b', 'x', 4),
    ('b', 'y', 6);
