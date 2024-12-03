-- pending: sqlite3 Need to build structs from scratch

CREATE OR REPLACE TABLE __result1 AS
SELECT STRUCT(1 AS a, 'b' AS b).*;

CREATE OR REPLACE TABLE __expected1 (
    a INT64,
    b STRING,
);
INSERT INTO __expected1 VALUES
  (1, 'b');
