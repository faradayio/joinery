CREATE OR REPLACE TABLE __result1 AS SELECT 'Hello, world!' AS greeting;

CREATE OR REPLACE TABLE __expected1 (greeting STRING);
INSERT INTO __expected1 VALUES ('Hello, world!');
