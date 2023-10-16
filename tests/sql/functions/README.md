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
- [ ] ARRAY_TO_STRING(_,_)
- [x] SUM(_)
- [x] FARM_FINGERPRINT(_)
- [ ] ANY_VALUE(_)
- [ ] ROW_NUMBER() OVER(..)
- [ ] COUNTIF(_)
- [x] UPPER(_)
- [ ] ARRAY_AGG(_)
- [ ] DATE_TRUNC(_,_) (special)
- [x] MIN(_)
- [ ] FORMAT_DATETIME(_,_)
- [ ] RAND()
- [ ] RANK() OVER(..)
- [ ] ARRAY_LENGTH(_)
- [ ] SUM(_) OVER(..)
- [ ] DATETIME_SUB(_,_)
- [ ] DATE_DIFF(_,_,_) (special)
- [ ] CURRENT_DATETIME()
- [ ] DATE_SUB(_,_)
- [ ] EXP(_)
- [x] MAX(_)
- [ ] GENERATE_UUID()
- [ ] DATE(_)
- [ ] LEAST(_,_)
- [ ] APPROX_QUANTILES(_,_)
- [ ] GENERATE_DATE_ARRAY(_,_,_)
- [ ] DATE_ADD(_,_)
- [ ] LAG(_) OVER(..)
- [ ] DATETIME_DIFF(_,_,_) (special)
- [ ] DATETIME_TRUNC(_,_) (special)
- [ ] FIRST_VALUE(_) OVER(..)
- [ ] DATETIME(_)
- [ ] LEAST(_)
