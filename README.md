# `joinery`: A BigQuery SQL transpiler experiment

Very incomplete.

Check out:

- `dbt-core`
- `sqlglot`

## Installing

```bash
cargo install --path .
```

## Running

```bash
joinery --help
joinery parse example_queries.csv
joinery sql-test tests/sql/
```

For the `sql-test` test format, see the [test format
docs](./tests/sql/README.md).

## Supported databases

### SQLite3

This is the default. You don't need to do anything.

### Snowflake

You can specify Snowflake using

```txt
--database snowflake://<user>@<organization>-<account>[.privatelink]/<warehouse>/<database>
```

You'll also need to set the `SNOWFLAKE_PASSWORD` environment variable.

## Grammar tracing

Run tests with tracing enabled:

```bash
cargo test --features trace
```

```bash
cargo install pegviz --git=https://github.com/fasterthanlime/pegviz.git
pegviz -o trace.html
```

Now take all the test output between these two lines, inclusive:

```txt
[PEG_INPUT_START]
...
[PEG_TRACE_STOP]
```

...and paste it into the standard input of `pegviz`. Then hit control-D. You should then be able to open `trace.html` in your browser and see a nice visualization of the grammar.
