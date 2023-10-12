-- IF
CREATE OR REPLACE TABLE __result1 AS
SELECT
    IF(TRUE, 1, 2) AS if_true,
    IF(FALSE, 1, 2) AS if_false,
    IF(NULL, 1, 2) AS if_null;

CREATE OR REPLACE TABLE __expected1 (
    if_true INT64,
    if_false INT64,
    if_null INT64,
);
INSERT INTO __expected1 VALUES
  (1, 2, 2);
