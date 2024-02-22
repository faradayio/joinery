-- pending: snowflake Not mapped.
-- pending: sqlite3 Not mapped.
--
-- SUBSTR

CREATE OR REPLACE TABLE __result1 AS
SELECT
    SUBSTR(CAST(NULL AS STRING), 1) AS substr2_null,
    SUBSTR(CAST(NULL AS STRING), 1, 2) AS substr3_null,

    -- Indexing is _normally_ 1-based. But an index of 0 is treated as if it
    -- were 1.
    SUBSTR('foobar', 0) AS substr2_1, -- 'foobar'
    SUBSTR('foobar', 1) AS substr2_2, -- 'foobar'
    SUBSTR('foobar', 4) AS substr2_3, -- 'bar'
    SUBSTR('foobar', -2) AS substr2_4, -- 'ar'
    SUBSTR('foobar', -6) AS substr2_5, -- 'foobar'
    -- Index of less than the negative length of the string is treated as 1.
    SUBSTR('foobar', -10) AS substr2_6, -- 'foobar'

    SUBSTR('foobar', 0, 3) AS substr3_1, -- 'foo'
    SUBSTR('foobar', 1, 3) AS substr3_2, -- 'foo'
    SUBSTR('foobar', 1, 6) AS substr3_3, -- 'foobar'
    -- If the length would extend past the end of the string, we return through
    -- the end of the string.
    SUBSTR('foobar', 1, 7) AS substr3_4, -- 'foobar'
    SUBSTR('foobar', -3, 1) AS substr3_5, -- 'b'
    SUBSTR('foobar', -3, 2) AS substr3_6, -- 'ba'
    SUBSTR('foobar', -3, 3) AS substr3_7, -- 'bar'
    -- More negative length cases.
    SUBSTR('foobar', -6, 6) AS substr3_8, -- 'foobar'
    -- More "past the end" cases
    SUBSTR('foobar', -3, 4) AS substr3_9, -- 'bar'
    SUBSTR('foobar', 0, 7) AS substr3_10, -- 'foobar'
    SUBSTR('foobar', -6, 7) AS substr3_11; -- 'foobar'

CREATE OR REPLACE TABLE __expected1 (
    substr2_null STRING,
    substr3_null STRING,

    substr2_1 STRING,
    substr2_2 STRING,
    substr2_3 STRING,
    substr2_4 STRING,
    substr2_5 STRING,
    substr2_6 STRING,

    substr3_1 STRING,
    substr3_2 STRING,
    substr3_3 STRING,
    substr3_4 STRING,
    substr3_5 STRING,
    substr3_6 STRING,
    substr3_7 STRING,
    substr3_8 STRING,
    substr3_9 STRING,
    substr3_10 STRING,
    substr3_11 STRING
);

INSERT INTO __expected1 VALUES (
    -- NULL cases.
    NULL, NULL,
    -- SUBSTR(_, _) cases.
    'foobar', 'foobar', 'bar', 'ar', 'foobar', 'foobar',
    -- SUBSTR(_, _, _) cases.
    'foo', 'foo', 'foobar', 'foobar', 'b', 'ba', 'bar', 'foobar', 'bar', 'foobar', 'foobar'
);
