# Function tests

This directory contains tests for BigQuery SQL functions, to see if we can run
them on other platforms.

## Tests to implement

Here is a list of functions that are high priorities to implement. You can
generate your own version of this list by running `joinery parse
--count-function-calls queries.csv`.

- [x] REGEXP_REPLACE(_,_,_)
- [x] REGEXP_EXTRACT(_,_)
- [x] COALESCE(*)
- [x] LOWER(_)
- [x] TO_HEX(_)
- [x] SHA256(_)
- [x] LENGTH(_)
- [x] CONCAT(*)
- [x] TRIM(_)
- [x] SUM(_)
- [x] FARM_FINGERPRINT(_)
- [x] ANY_VALUE(_)
- [x] ROW_NUMBER() OVER(..)
- [x] COUNTIF(_)
- [x] UPPER(_)
- [x] MIN(_)
- [x] RAND()
- [x] RANK() OVER(..)
- [x] SUM(_) OVER(..)
- [x] EXP(_)
- [x] MAX(_)
- [x] GENERATE_UUID()
- [x] LEAST(*)
- [x] APPROX_QUANTILES(_,_)
- [x] LAG(_) OVER(..)
- [x] FIRST_VALUE(_) OVER(..)

Arrays:

- [x] ARRAY_TO_STRING(_,_)
- [x] ARRAY_AGG(_)
- [x] ARRAY_LENGTH(_)
- [x] OFFSET
- [x] ORDINAL
- [x] ARRAY(SELECT ...)

Special date functions:

- [ ] CURRENT_DATETIME()
- [ ] DATE(_)
- [ ] DATE_ADD(_,_)
- [ ] DATE_DIFF(_,_,_) (special)
- [ ] DATE_SUB(_,_)
- [ ] DATE_TRUNC(_,_) (special)
- [ ] DATETIME(_)
- [ ] DATETIME_DIFF(_,_,_) (special)
- [ ] DATETIME_SUB(_,_)
- [ ] DATETIME_TRUNC(_,_) (special)
- [ ] FORMAT_DATETIME(_,_)
- [ ] GENERATE_DATE_ARRAY(_,_,_)
- [ ] INTERVAL

## Operators and special forms

- [x] SAFE_CAST
- [x] IN UNNEST
- [x] IN (SELECT ...)
