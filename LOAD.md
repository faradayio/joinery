All SQL must be run through joinery.
Every time that we create a table, we need to insert its BigQuery `CREATE TABLE name (col...)` into pg.
That table looks like this:

```sql
PRIMARY KEY (bq_project, bq_dataset, bq_table_name)

bq_project -- The BigQuery project, which gets mapped to a Trino catalog somehow
bq_dataset -- The BigQuery dataset, which is a trino schema with the same name
bq_table_name -- The BigQuery table, which is a trino table with the same name
create_table_sql -- this is always a raw typed CREATE TABLE statement in BigQuery SQL. CREATE TABLE (my_col data_type, ...);
```

When we run another SQL query tomorrow, we need to make sure that we have access to the BigQuery table names and their `CREATE TABLE` SQL
We can load those table definitions into our scope
Then run type inference normally

So when we try to access prod_gke.my_dataset.my_table,
...we find a CREATE TABLE for it, parse it, and inject it in the scope
