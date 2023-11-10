-- pending: snowflake Lots of work but low risk.
-- pending: sqlite3 Lots of work but low risk.
-- pending: trino Work in progress

-- Moon landing UTC: 1969-07-20T20:17:39Z

-- We can't test these against specific values, but we want to at least run
-- them.
SELECT
    -- BigQuery supports both.
    CURRENT_DATETIME,
    CURRENT_DATETIME();

CREATE OR REPLACE TABLE __result1 AS
SELECT
    DATETIME('1969-07-20 20:17:39') AS moon_landing_datetime,
    DATETIME(DATE('1969-07-20')) AS moon_landing_date,

    -- The initial versions of these test cases are carefully chosen to _avoid_
    -- boundary conditions. If anyone wants to write really rigorous test cases
    -- that cover all the boundary conditions, and make sure they pass for
    -- all/most dialects, that would be amazing.
    DATETIME_DIFF(DATETIME('1969-07-20 20:17:39'), DATETIME('1969-07-20 20:17:39'), SECOND) AS same_time,
    DATETIME_DIFF(DATETIME('1969-07-20 20:17:49'), DATETIME('1969-07-20 20:17:39'), SECOND) AS ten_seconds_later,
    DATETIME_DIFF(DATETIME('1969-07-20 20:17:39'), DATETIME('1969-07-20 20:17:49'), SECOND) AS ten_seconds_earlier,
    DATETIME_DIFF(DATETIME('1969-07-20 20:18:39'), DATETIME('1969-07-20 20:17:39'), MINUTE) AS one_minute_later,
    DATETIME_DIFF(DATETIME('1969-07-20 21:17:39'), DATETIME('1969-07-20 20:17:39'), HOUR) AS one_hour_later,
    DATETIME_DIFF(DATETIME('1969-07-21 20:17:39'), DATETIME('1969-07-20 20:17:39'), DAY) AS one_day_later,
    DATETIME_DIFF(DATETIME('1969-08-20 20:17:39'), DATETIME('1969-07-20 20:17:39'), MONTH) AS one_month_later,
    DATETIME_DIFF(DATETIME('1970-07-20 20:17:39'), DATETIME('1969-07-20 20:17:39'), YEAR) AS one_year_later,

    DATETIME_TRUNC(DATETIME('1969-07-20 20:17:39'), MINUTE) AS trunc_minute,
    DATETIME_TRUNC(DATETIME('1969-07-20 20:17:39'), HOUR) AS trunc_hour,
    DATETIME_TRUNC(DATETIME('1969-07-20 20:17:39'), DAY) AS trunc_day,

    DATETIME_SUB(DATETIME('1969-07-20 20:17:39'), INTERVAL 10 SECOND) AS sub_ten_seconds_earlier,
    DATETIME_SUB(DATETIME('1969-07-20 20:17:39'), INTERVAL 1 MINUTE) AS sub_one_minute_earlier,
    ;

CREATE OR REPLACE TABLE __expected1 (
    moon_landing_datetime DATETIME,
    moon_landing_date DATETIME,

    same_time INT64,
    ten_seconds_later INT64,
    ten_seconds_earlier INT64,
    one_minute_later INT64,
    one_hour_later INT64,
    one_day_later INT64,
    one_month_later INT64,
    one_year_later INT64,

    trunc_minute DATETIME,
    trunc_hour DATETIME,
    trunc_day DATETIME,

    sub_ten_seconds_earlier DATETIME,
    sub_one_minute_earlier DATETIME,
);
INSERT INTO __expected1 VALUES (
    DATETIME('1969-07-20 20:17:39'), -- moon_landing_datetime
    DATETIME('1969-07-20 00:00:00'), -- moon_landing_date

    0, -- same_time
    10, -- ten_seconds_later
    -10, -- ten_seconds_earlier
    1, -- one_minute_later
    1, -- one_hour_later
    1, -- one_day_later
    1, -- one_month_later
    1, -- one_year_later

    DATETIME('1969-07-20 20:17:00'), -- trunc_minute
    DATETIME('1969-07-20 20:00:00'), -- trunc_hour
    DATETIME('1969-07-20 00:00:00'), -- trunc_day

    DATETIME('1969-07-20 20:17:29'), -- sub_ten_seconds_earlier
    DATETIME('1969-07-20 20:16:39'), -- sub_one_minute_earlier
);