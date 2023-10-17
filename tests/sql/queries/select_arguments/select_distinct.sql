-- SELECT DISTINCT

CREATE TEMP TABLE duplicated_values (i INT64);
INSERT INTO duplicated_values VALUES (1), (1), (2), (2);

CREATE OR REPLACE TABLE __result1 AS
SELECT DISTINCT i FROM duplicated_values;

CREATE OR REPLACE TABLE __expected1 (
    i INT64,
);
INSERT INTO __expected1 VALUES
  (1),
  (2);
