CREATE TABLE __result1 AS
SELECT
    NULL AS n,
    1 AS i,
    1.0 AS f,
    'Hello, world!' AS s,
    TRUE AS b1,
    FALSE AS b2;
    
CREATE TABLE __expected1 (
    n INT64,
    i INT64,
    f FLOAT64,
    s STRING,
    b1 BOOL,
    b2 BOOL,
);
INSERT INTO __expected1 VALUES
  (NULL, 1, 1.0, 'Hello, world!', TRUE, FALSE);