-- IS operator tests.
CREATE OR REPLACE TABLE __result1 AS
SELECT
    1 IS NULL AS one_is_null,
    1 IS NOT NULL AS one_is_not_null,
    NULL IS NULL AS null_is_null,
    NULL IS NOT NULL AS null_is_not_null,
    TRUE IS TRUE AS true_is_true,
    NULL IS TRUE AS null_is_true,
    FALSE IS FALSE AS false_is_false,
    NULL IS FALSE AS null_is_false,
    NULL IS UNKNOWN AS null_is_unknown,
    NULL IS NOT UNKNOWN AS null_is_not_unknown,
    1 IS UNKNOWN AS one_is_unknown;

CREATE OR REPLACE TABLE __expected1 (
    one_is_null BOOL,
    one_is_not_null BOOL,
    null_is_null BOOL,
    null_is_not_null BOOL,
    true_is_true BOOL,
    null_is_true BOOL,
    false_is_false BOOL,
    null_is_false BOOL,
    null_is_unknown BOOL,
    null_is_not_unknown BOOL,
    one_is_unknown BOOL,
);
INSERT INTO __expected1 VALUES (
    FALSE, -- one_is_null
    TRUE, -- one_is_not_null
    TRUE, -- null_is_null
    FALSE, -- null_is_not_null
    TRUE, -- true_is_true
    FALSE, -- null_is_true
    TRUE, -- false_is_false
    FALSE, -- null_is_false
    TRUE, -- null_is_unknown
    FALSE, -- null_is_not_unknown
    FALSE -- one_is_unknown
);