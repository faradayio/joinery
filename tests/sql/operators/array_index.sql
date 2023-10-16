-- pending: sqlite3 No array support.

-- We pre-declare this table to avoid SELECT creating a VARIANT column, which
-- breaks our Snowflake test driver (though it appears to store correctly).
CREATE OR REPLACE TABLE __result1 (
    array_index INT64,
    array_offset INT64,
    array_ordinal INT64,
);
INSERT INTO __result1
SELECT
    [1,2][1] AS array_index,
    [1,2][OFFSET(1)] AS array_offset,
    [1,2][ORDINAL(1)] AS array_ordinal;

CREATE OR REPLACE TABLE __expected1 (
    array_index INT64,
    array_offset INT64,
    array_ordinal INT64,
);
INSERT INTO __expected1 VALUES
  (2, 2, 1);
