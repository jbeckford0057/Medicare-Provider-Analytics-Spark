# Processed Data

Processed Spark outputs are intentionally not stored in GitHub because they contain large Parquet datasets and partition directories.

The final integrated master dataset is stored in Amazon S3 under:

`s3://medicare-provider-analytics-cs675-2026/processed/master_provider_services/`

The integrated dataset contains **9,781,673 rows** and is partitioned by provider state for downstream Athena and Spark analytics.
