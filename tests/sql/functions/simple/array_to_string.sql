-- pending: sqlite3 Array functions are not available.
--
-- ARRAY_TO_STRING

CREATE OR REPLACE TABLE __result1 AS
SELECT
    ARRAY_TO_STRING([1, 2, 3], ',') AS array_int_to_string,
    ARRAY_TO_STRING(['a', 'b', 'c'], ',') AS array_string_to_string;

CREATE OR REPLACE TABLE __expected1 (
    array_int_to_string STRING,
    array_string_to_string STRING,
);
INSERT INTO __expected1 VALUES
  ('1,2,3', 'a,b,c');
