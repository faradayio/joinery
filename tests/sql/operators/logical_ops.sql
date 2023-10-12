CREATE OR REPLACE TABLE __result1 AS
SELECT
    NOT TRUE AS not_true,
    NOT FALSE AS not_false,
    NOT NULL AS not_null,
    TRUE AND FALSE AS `and`,
    TRUE OR FALSE AS `or`;

CREATE OR REPLACE TABLE __expected1 (
    not_true BOOL,
    not_false BOOL,
    not_null BOOL,
    `and` BOOL,
    `or` BOOL,
);
INSERT INTO __expected1 VALUES
  (FALSE, TRUE, NULL, FALSE, TRUE);
