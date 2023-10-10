-- Comparisons
CREATE TABLE __result1 AS
SELECT
    1 = 1 AS eq,
    1 != 1 AS ne,
    1 < 1 AS lt,
    1 <= 1 AS le,
    1 > 1 AS gt,
    1 >= 1 AS ge,
    1 IS NULL AS is_null,
    1 IS NOT NULL AS is_not_null;

CREATE TABLE __expected1 (
    eq BOOL,
    ne BOOL,
    lt BOOL,
    le BOOL,
    gt BOOL,
    ge BOOL,
    is_null BOOL,
    is_not_null BOOL,
);
INSERT INTO __expected1 VALUES
  (TRUE, FALSE, FALSE, TRUE, FALSE, TRUE, FALSE, TRUE);
