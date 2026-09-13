# SQL and Athena Queries

This directory contains the SQL artifacts used for the Amazon Athena portion of the Medicare Provider Analytics project.

## Files

- `create_athena_table.sql` – creates the external Athena table over the processed Parquet dataset in Amazon S3.
- `register_partitions.sql` – registers state-based partitions.
- `athena_validation_queries.sql` – validates row counts and demonstrates partition-pruning behavior.

These files are reproducible deployment artifacts for the completed Athena implementation.
