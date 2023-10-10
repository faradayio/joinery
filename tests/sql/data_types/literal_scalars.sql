CREATE TABLE __result1 AS
SELECT
    1 AS i,
    1.0 AS f,
    'Hello, world!' AS s,
    TRUE AS b;
    
CREATE TABLE __expected1 (
    i INT64,
    f FLOAT64,
    s STRING,
    b BOOL
);
INSERT INTO __expected1 VALUES
  (1, 1.0, 'Hello, world!', TRUE);