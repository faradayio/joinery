CREATE OR REPLACE TABLE __result1 AS
SELECT
    length('日本国') AS str_len,
    -- Snowflake requires `to_binary(s, 'utf-8')`. CAST 
    -- assumes the string is hex-encoded.
    --
    --length(CAST('日本国' AS BYTES)) AS bytes_len,
    length(null) AS null_len;

CREATE OR REPLACE TABLE __expected1 (
    str_len INT64,
    --bytes_len INT64,
    null_len INT64,
);
INSERT INTO __expected1 VALUES
  (3, /*9,*/ NULL);
