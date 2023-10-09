CREATE TABLE __result1 AS SELECT 'Hello, world!' AS greeting;

CREATE TABLE __expected1 (greeting STRING);
INSERT INTO __expected1 VALUES ('Hello, world!');
