-- CASE WHEN
CREATE OR REPLACE TABLE __result1 AS
SELECT
    CASE WHEN TRUE THEN 1 ELSE 2 END AS case_when_true,
    CASE WHEN FALSE THEN 1 ELSE 2 END AS case_when_false,
    CASE WHEN NULL THEN 1 ELSE 2 END AS case_when_null,
    CASE WHEN FALSE THEN 1 END AS case_when_false_no_else,
    CASE WHEN FALSE THEN 1 WHEN TRUE THEN 2 ELSE 3 END AS case_when_false_true_else;

CREATE OR REPLACE TABLE __expected1 (
    case_when_true INT64,
    case_when_false INT64,
    case_when_null INT64,
    case_when_false_no_else INT64,
    case_when_false_true_else INT64,
);
INSERT INTO __expected1 VALUES
  (1, 2, 2, NULL, 2);
