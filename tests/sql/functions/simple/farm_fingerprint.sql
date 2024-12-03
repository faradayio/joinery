-- pending: sqlite3 FARM_FINGERPRINT only exists on BigQuery
--
-- This works on Trino if you load the UDF as described in `./java/README.md`.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    FARM_FINGERPRINT('foo') AS str_farm,
    FARM_FINGERPRINT(null) AS null_farm;

CREATE OR REPLACE TABLE __expected1 (
    str_farm INT64,
    null_farm INT64,
);
INSERT INTO __expected1 VALUES
  (6150913649986995171, NULL);