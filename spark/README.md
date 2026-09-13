# Spark Scripts

This directory contains the reusable Spark and EMR artifacts used by the Medicare Provider Analytics project.

## Executed on Amazon EMR on EC2

- `emr_validation.py` – validates the processed Parquet master dataset in Amazon S3. This script was executed successfully on Amazon EMR on EC2 using Spark 3.5.8 and completed with exit code 0.

## Additional Validation and Scalability Workloads

- `emr_raw_join_validation.py` – reproduces the raw cross-source NPI join and writes compact validation evidence.
- `scale_benchmark.py` – generates a transparent synthetic 100M+ logical-row Spark benchmark.

## EMR Serverless Configuration Artifacts

- `emr_application_config.json`
- `emr_configuration_overrides.json`
- `emr_job_driver.json`
- `emr_raw_join_job_driver.json`
- `emr_scale_benchmark_job_driver.json`

These configuration files document the originally prepared EMR Serverless path. The final successful AWS Spark execution used Amazon EMR on EC2 because the account-level EMR Serverless vCPU quota request remained under AWS review.
