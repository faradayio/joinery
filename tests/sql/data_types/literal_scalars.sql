
CREATE OR REPLACE TABLE __result1 AS
SELECT
    -- Trino doesn't like untyped NULLs.
    CAST(NULL AS INT64) AS n,
    1 AS i,
    -- TODO: Both our Trino and our Snowflake drivers are unable to correctly
    -- read columns with types inferred from float literals. We may want to add
    -- a transformation that automatically casts float literals to FLOAT64.
    CAST(1.5 AS FLOAT64) AS f,
    'Hello, world!' AS s,
    '\a\b\f\n\r\t\v\\\?\'\"\`\101\x41\X41\u0041\U00000041' AS escapes, 
    r'\a' AS raw1,
    r"\a" AS raw2,
    TRUE AS b1,
    FALSE AS b2;
    
CREATE OR REPLACE TABLE __expected1 (
    n INT64,
    i INT64,
    f FLOAT64,
    s STRING,
    escapes STRING,
    raw1 STRING,
    raw2 STRING,
    b1 BOOL,
    b2 BOOL,
);
INSERT INTO __expected1 VALUES
  (NULL, 1, 1.5, 'Hello, world!', '\a\b\f\n\r\t\v\\?\'"`AAAAA', '\\a', '\\a', TRUE, FALSE);