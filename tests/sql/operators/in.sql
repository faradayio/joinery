-- IN, NOT IN
CREATE TABLE __result1 AS
SELECT
    1 IN (1, 2, 3) AS in1,
    1 IN (2, 3) AS in2,
    1 IN (SELECT 1) AS in3,
    1 IN (SELECT 2) AS in4,
    1 NOT IN (1, 2, 3) AS not_in1,
    1 NOT IN (2, 3) AS not_in2,
    1 NOT IN (SELECT 1) AS not_in3,
    1 NOT IN (SELECT 2) AS not_in4;

CREATE TABLE __expected1 (
    in1 BOOL,
    in2 BOOL,
    in3 BOOL,
    in4 BOOL,
    not_in1 BOOL,
    not_in2 BOOL,
    not_in3 BOOL,
    not_in4 BOOL,
);
INSERT INTO __expected1 VALUES
  (TRUE, FALSE, TRUE, FALSE, FALSE, TRUE, FALSE, TRUE);
