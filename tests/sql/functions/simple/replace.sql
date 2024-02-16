-- pending: sqlite3 Probably exists, but haven't checked.

CREATE OR REPLACE TABLE __result1 AS
SELECT
    replace('hello world', 'world', 'universe') AS replace1;

CREATE OR REPLACE TABLE __expected1 (
    replace1 STRING
);
INSERT INTO __expected1 VALUES
    ('hello universe');
