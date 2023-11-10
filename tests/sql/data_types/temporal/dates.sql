-- pending: snowflake Lots of work but low risk.
-- pending: sqlite3 Lots of work but low risk.

-- Moon landing UTC: 1969-07-20T20:17:39Z

-- We can't test these against specific values, but we want to at least run
-- them.
SELECT
    -- BigQuery supports both.
    CURRENT_DATE,
    CURRENT_DATE();

CREATE OR REPLACE TABLE __result1 AS
SELECT
    DATE('1969-07-20') AS moon_landing_date,

    -- The initial versions of these test cases are carefully chosen to _avoid_
    -- boundary conditions. If anyone wants to write really rigorous test cases
    -- that cover all the boundary conditions, and make sure they pass for
    -- all/most dialects, that would be amazing.
    DATE_DIFF(DATE('1969-07-20'), DATE('1969-07-20'), DAY) AS same_time,
    DATE_DIFF(DATE('1969-07-21'), DATE('1969-07-20'), DAY) AS one_day_later,
    DATE_DIFF(DATE('1969-08-20'), DATE('1969-07-20'), MONTH) AS one_month_later,
    DATE_DIFF(DATE('1970-07-20'), DATE('1969-07-20'), YEAR) AS one_year_later,

    DATE_TRUNC(DATE('1969-07-20'), MONTH) AS trunc_month,
    DATE_TRUNC(DATE('1969-07-20'), YEAR) AS trunc_year,

    DATE_ADD(DATE('1969-07-20'), INTERVAL 10 DAY) AS add_ten_days_later,
    
    DATE_SUB(DATE('1969-07-20'), INTERVAL 10 DAY) AS sub_ten_days_earlier,
    ;

CREATE OR REPLACE TABLE __expected1 (
    moon_landing_date DATE,

    same_time INT64,
    one_day_later INT64,
    one_month_later INT64,
    one_year_later INT64,

    trunc_month DATE,
    trunc_year DATE,

    add_ten_days_later DATE,

    sub_ten_days_earlier DATE,
);
INSERT INTO __expected1 VALUES (
    DATE('1969-07-20'), -- moon_landing_date

    0, -- same_time
    1, -- one_day_later
    1, -- one_month_later
    1, -- one_year_later

    DATE('1969-07-01'), -- trunc_month
    DATE('1969-01-01'), -- trunc_year

    DATE('1969-07-30'), -- add_ten_days_later

    DATE('1969-07-10'), -- sub_ten_days_earlier
);