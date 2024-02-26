# `joinery`: Safe SQL transpiler, written in Rust

It was decided to write a greenfield transpiler in Rust due to concerns about correctness of Python-based solutions.

[BigQuery "Standard SQL"](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax) was taken as the reference dialect, but it is anticipated the other input dialects will be supported.

It performs type inference (necessary, for example, to expand `EXCEPT(*)` into a list of columns, because Trino doesn't support it) and preserves whitespace.

If you want to run _your_ production workloads, **you will almost certainly need to contribute code.** In particular, our API coverage is limited. See [`tests/sql/`](./tests/sql/) for examples of what we support.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for an overview of the codebase.

```
$ joinery --help
Usage: joinery <COMMAND>

Commands:
  parse      Parse SQL from a CSV file containing `id` and `query` columns
  sql-test   Run SQL tests from a directory
  transpile  Transpile BigQuery SQL to another dialect
  help       Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

## Status

- Trino has passing unit tests for all our use cases, but probably not yours. Also, there's a difference between "works with the SQL test suite that _theoretically_ covers the features we support" and "works with gnarly production queries that do tricky things with correlated subqueries."
- AWS Athena 3 is basically Trino, except UDFs are different and we don't support them yet. There may also be dialect differences. Not currently tested.
- Snowflake has partial support.

## What is this?

This is an experimental tool to transpile (some) SQL code written in BigQuery's "Standard SQL" dialect into other dialects. For example, it can transform:

```sql
SELECT ARRAY(
    SELECT DISTINCT val / 2
    FROM UNNEST(arr) AS val
    WHERE MOD(val, 2) = 0
) AS arr
FROM array_select_data;
```

...into the Trino-compatible SQL:

```sql
SELECT ARRAY_DISTINCT(
    TRANSFORM(
        FILTER(arr, val -> MOD(val, 2) = 0),
        val -> val / 2
    )
) AS arr
FROM array_select_data
```

It even does type inference, which is needed for certain complex transformations! The transformation process makes some effort to preserve whitespace and comments, so the output SQL is still mostly readable.

## Design philosophy

In an _ideal_ world, `joinery` would do one of two things:

1. Translate your SQL into something that runs identically on your target database, or
2. Report a clear error explaining why it can't.

In the real world, neither BigQuery's Standard SQL nor any of our target dialects have any kind of formal semantics, and there has been way too much empiricism and guesswork involved. But `joinery` has been designed to approach the ideal over time.

## Installing

```bash
git clone https://github.com/faradayio/joinery.git
cd joinery
cargo install --path .
```

## Running

```bash
joinery --help
joinery parse example_queries.csv
joinery sql-test tests/sql/
```

For the `sql-test` test format, see the [test format docs][tests].

[tests]: ./tests/sql/README.md

## Supported databases

### SQLite3

This is the default. You don't need to do anything. It's not very useful, and will either be removed or replaced with DuckDB.

### Trino

To run under Trino, you may want to load the plugin, as described in [`java/trino-plugin/README.md`](./java/trino-plugin/README.md). For local testing, make sure you `curl` and [`asdf`](https://asdf-vm.com/) installed, and then install `just`:

```bash
cargo install just
```

Then you can run:

```bash
just docker-run-trino
```

From here, you can run the unit tests:

```bash
just check-trino
```

Then you need to start a Trino shell:

```bash
just trino-shell
```

For more details on all these commands, see the [`Justfile`](./Justfile).

### Snowflake

This hasn't been updated recently, so some tests for newer features may fail.

You can specify Snowflake using

```txt
--database snowflake://<user>@<organization>-<account>[.privatelink]/<warehouse>/<database>
```

You'll also need to set the `SNOWFLAKE_PASSWORD` environment variable.

## Developing

See [ARCHITECTURE.md](./ARCHITECTURE.md) for an overview of the codebase.

## Other projects of interest

If you're interested in running analytic SQL queries across multiple databases, you may also be interested in:

- [PRQL](https://prql-lang.org/). There are a lot of languages out there that compile to SQL, but this one is my favorite. It reads as a pipeline from top to bottom, the features are clean and orthogonal, and it handles window functions. If you're starting a greenfield project, definitely take a look.
- [Logica](https://logica.dev/). This is probably the most mature Datalog-to-SQL compiler, with a particular focus on BigQuery.
- [`sqlglot`](https://github.com/tobymao/sqlglot). Transform between many different SQL dialects. Much better feature coverage than we have, though it may generate incorrect SQL in tricky cases. If you're planning on adjusting your translated queries by hand, or if you need to support a wide variety of dialects, this is probably a better choice than `joinery`.
- [`dbt-core`](https://github.com/dbt-labs/dbt-core).
- [BigQuery Emulator](https://github.com/goccy/bigquery-emulator). A local emulator for BigQuery. This supports a larger fraction of BigQuery features than we do.

## Corporate support

joinery is open-sourced by [Faraday](https://faraday.ai)
