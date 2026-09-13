# Project Architecture

## Overview

The Medicare Provider Analytics project uses Apache Spark for large-scale data preparation, integration, validation, and analytics. Development was completed locally and the final cloud implementation was validated on AWS.

## Local Development Architecture

```text
CMS CSV Data
    ↓
Apache Spark / PySpark
    ↓
Profiling and Cleaning
    ↓
Feature Engineering
    ↓
NPI-Based Integration
    ↓
Partitioned Parquet
    ↓
Local Analytics / Validation
```

Local tools included Python, JupyterLab, Apache Spark, Git, and GitHub.

## Final AWS Architecture

```text
CMS Source Data
    ↓
Amazon S3
    ↓
Apache Spark on Amazon EMR on EC2
    ↓
Processed / Integrated Parquet
    ↓
Amazon S3
   ↙       ↘
Athena     EMR Spark Validation
   ↓             ↓
SQL Results   YARN / Cloud Monitoring
```

### AWS Components

- Amazon S3 for raw, processed, analytics, script, log, and query-result storage
- Amazon Athena for serverless SQL analytics
- Amazon EMR on EC2 for Spark 3.5.8 execution
- Hadoop 3.4.2 and YARN for cluster execution
- IAM service role and EC2 instance profile for controlled AWS access
- EMR/CloudWatch monitoring for runtime and idle-state evidence

The successful EMR validation used one `r8g.xlarge` Primary node with no Core or Task nodes. The Spark validation step completed successfully and the cluster later auto-terminated after becoming idle.
