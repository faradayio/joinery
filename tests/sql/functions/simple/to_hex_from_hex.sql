-- TO_HEX(FROM_HEX(x)) = x

CREATE OR REPLACE TABLE __result1 AS
SELECT
    to_hex(from_hex('8f4343a7')) AS hex_arg,
    to_hex(from_hex(null)) AS null_arg;

CREATE OR REPLACE TABLE __expected1 (
    hex_arg STRING,
    null_arg STRING,
);

INSERT INTO __expected1 VALUES
  ('8f4343a7', NULL);

