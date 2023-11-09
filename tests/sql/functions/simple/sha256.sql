-- pending: sqlite3 No SHA256 function

CREATE OR REPLACE TABLE __result1 AS
SELECT
    to_hex(sha256('hi')) AS hi,
    to_hex(sha256(null)) AS null_arg;

CREATE OR REPLACE TABLE __expected1 (
    hi STRING,
    null_arg STRING,
);
INSERT INTO __expected1 VALUES
  ('8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4', NULL);
