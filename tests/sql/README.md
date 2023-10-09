# SQL Test Suite

This is a simple test suite for SQL databases. It's entirely written in BigQuery-compatible "Standard SQL."

## Conventions

Tests may contain multiple SQL statements. Statements must be separated by semicolons.

Each test must create one or more pairs of tables (or views), named `__result{N}` and `__expected{N}`. The test is considered to have passed if the two tables are identical. Tables will be sorted into consistent order before comparison.

```sql
CREATE TABLE __result1 AS SELECT 1+1 AS a;

CREATE TABLE __expected1 (a INT64);
INSERT INTO __expected1 VALUES (2);
```

When run:

- All code will be run with `default_project` and `default_dataset` set to an appropriate test project and dataset.
- The tables named `__result{N}` and `__expected{N}` will be renamed to include the test name.
- Any other required tables or views should be created with `CREATE TEMP TABLE` or `CREATE TEMP VIEW`. This means that when running against BigQuery, all SQL will be run in a single session.
- The test harness will automatically clean up any tables or views created by the test. You should not attempt to create any tables or views in other datasets or projects.
