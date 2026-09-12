# Medicare Provider Analytics Using Apache Spark and AWS

## CS675 – Big Data: Management & Analytics

**Project:** Medicare Provider Analytics Using Apache Spark and AWS  
**Course:** CS675 – Big Data: Management & Analytics  
**Institution:** King Graduate School  
**Student:** Judi-Ann Beckford

---

## Project Overview

This project implements an end-to-end big data analytics pipeline using publicly available Medicare provider datasets from the Centers for Medicare & Medicaid Services (CMS).

The solution integrates Medicare provider-service utilization data with Medicare Fee-for-Service provider enrollment data using the National Provider Identifier (NPI). Apache Spark is used for distributed profiling, preprocessing, feature engineering, integration, validation, and analytics. The final cloud implementation uses Amazon S3 for object storage, Amazon Athena for serverless SQL analytics, and Amazon EMR on EC2 for AWS-hosted Spark execution.

The project demonstrates:

- Distributed data processing with Apache Spark
- Data profiling and quality assessment
- Missing-value and outlier handling
- Feature engineering
- Cross-source integration using NPI
- Join validation and match-rate analysis
- Parquet optimization
- State-based partitioning
- Amazon S3 cloud storage
- Amazon Athena serverless analytics
- Partition-pruning performance analysis
- Amazon EMR on EC2 Spark execution
- YARN-based distributed job execution
- Cloud monitoring and automatic cluster termination
- Synthetic 100M+ row scalability benchmarking
- Reproducible project artifacts in GitHub

---

## Data Sources

### 1. Medicare Physician & Other Practitioners – by Provider and Service

This dataset contains Medicare utilization, beneficiary, service, charge, allowed-payment, and Medicare-payment information at the provider/service level.

Key fields used include:

- NPI
- Provider specialty
- Provider state
- HCPCS code
- HCPCS description
- Place of service
- Total beneficiaries
- Total services
- Average submitted charge
- Average Medicare allowed amount
- Average Medicare payment
- Average standardized Medicare payment

### 2. Medicare Fee-for-Service Public Provider Enrollment

This dataset contains provider enrollment information used to enrich the provider-service dataset.

Key information includes:

- NPI
- Provider type
- Provider name or organization
- Enrollment state
- Enrollment records

Because an NPI can occur more than once in the enrollment source, the enrollment data is aggregated into a **one-row-per-NPI enrollment dimension** before joining it to the provider-service dataset. This prevents unintended row multiplication.

---

## Research Questions

1. Which provider specialties account for the highest estimated Medicare payments?
2. Which HCPCS procedure codes account for the highest estimated Medicare payments?
3. Which states have the highest Medicare service utilization and estimated payment activity?
4. Which enrollment provider types account for the highest service utilization?
5. Which provider records may warrant additional analytical review based on unusually high estimated payment activity?

The project does **not** classify providers as fraudulent. High-payment or high-utilization records are treated only as candidates for further analytical review.

---

## Data Integration Strategy

The two CMS sources are integrated using the **National Provider Identifier (NPI)**.

The provider-service dataset contains multiple service-level records for an individual NPI. The enrollment source may also contain multiple enrollment records for the same NPI. To avoid a many-to-many join, the enrollment source is transformed into a one-row-per-NPI dimension before integration.

The final integration uses a **left outer join** from provider-service data to the enrollment dimension.

Validated local Spark results:

- Final integrated rows: **9,781,673**
- Enrollment dimension: **2,556,656 unique NPIs**
- Matched provider-service rows: **9,645,673**
- Unmatched provider-service rows: **136,000**
- Match rate: **98.61%**
- Join row preservation: **PASS**

The local Spark physical execution plan used a **SortMergeJoin**.

---

## System Architecture

### Local Development Layer

The local environment was used to develop, test, debug, and validate the Spark pipeline before cloud execution.

Technologies used:

- Python
- PySpark
- Apache Spark
- JupyterLab
- MinIO for local S3-compatible object-storage testing
- Git and GitHub

MinIO served only as a local development and testing environment.

### AWS Cloud Layer

The final AWS implementation uses:

- **Amazon S3** – raw, processed, analytical, EMR-script, and query-result storage
- **Amazon Athena** – serverless SQL querying of partitioned Parquet data
- **Amazon EMR on EC2** – cloud-hosted Spark 3.5.8 execution
- **Apache Hadoop 3.4.2 / YARN** – cluster resource management and Spark application execution
- **AWS IAM** – EMR service role and EC2 instance profile
- **Amazon CloudWatch / EMR monitoring** – runtime and idle-state monitoring

Primary AWS region:

`us-east-1`

### Final Data Flow

`CMS Raw Data → Apache Spark → Cleaning & Feature Engineering → NPI Integration → Partitioned Parquet → Amazon S3 → Amazon Athena + Amazon EMR on EC2 → Analytical Results`

---

## Apache Spark Processing Pipeline

The project is organized into four primary Jupyter notebooks.

### 1. Dataset Profiling

`notebooks/01_dataset_profiling.ipynb`

Activities include schema inspection, row/column analysis, missing-value assessment, descriptive statistics, integration-key review, and initial quality checks.

### 2. Data Cleaning and Feature Engineering

`notebooks/02_data_cleaning.ipynb`

Processing includes data-type standardization, missing-value treatment, numeric quality checks, outlier treatment, capped measures, normalized features, categorical preparation, and utilization-band creation.

### 3. Cross-Source Data Integration

`notebooks/03_data_integration.ipynb`

The integration pipeline standardizes NPI, creates a one-row-per-NPI enrollment dimension, performs the left outer join, validates row preservation, calculates match statistics, evaluates state consistency, and writes the master dataset as Parquet partitioned by `PROVIDER_STATE`.

### 4. Medicare Analytics

`notebooks/04_medicare_analytics.ipynb`

The analytical notebook evaluates provider specialties, HCPCS procedures, geographic utilization, enrollment provider types, and high-payment records.

An analytical estimate is calculated as:

`ESTIMATED_TOTAL_MEDICARE_PAYMENT = TOTAL_SERVICES × AVG_MEDICARE_PAYMENT`

This is an analytical estimate, not an official CMS total-payment field.

---

## Amazon S3 Implementation

Project bucket:

`medicare-provider-analytics-cs675-2026`

Logical prefixes include:

```text
raw/
├── provider_services/
└── provider_enrollment/

processed/
└── master_provider_services/

analytics/
athena-results/

emr/
├── scripts/
├── output/
└── logs/
```

Large source and processed datasets are intentionally excluded from GitHub and stored in Amazon S3.

---

## Amazon Athena

Athena is used to query the processed Parquet master dataset stored in S3.

Repository SQL artifacts:

- `sql/create_athena_table.sql`
- `sql/register_partitions.sql`
- `sql/athena_validation_queries.sql`

The Athena implementation validates the integrated row count and demonstrates partition pruning for state-filtered queries.

---

## Amazon EMR on EC2 – Successful Cloud Spark Execution

The final Spark cloud validation was executed successfully on **Amazon EMR on EC2**.

### Cluster Configuration

- Amazon EMR release: **emr-7.14.0**
- Apache Spark: **3.5.8**
- Hadoop: **3.4.2**
- Cluster topology: **1 Primary / 0 Core / 0 Task**
- Primary instance type: **r8g.xlarge**
- Automatic idle termination: **10 minutes**
- S3 cluster logging: enabled

### Successful Validation Step

Spark script:

`spark/emr_validation.py`

EMR step name:

`Medicare-Processed-Data-Validation`

Execution result:

- Step status: **Completed**
- YARN application successfully submitted and executed
- Controller exit code: **0**
- Spark step runtime: **128 seconds**
- Cluster returned to an idle state after completion
- Automatic termination completed successfully

This confirms that the processed Medicare dataset and Spark validation workflow executed successfully in AWS, not only in the local development environment.

### AWS EMR Execution Evidence

The repository includes visual evidence of the successful EMR lifecycle in `images/aws-evidence/`:

- `01-emr-cluster-configuration.png` – EMR cluster configuration and installed applications
- `01.1-emr-cluster-running.png` – cluster running in AWS
- `02-emr-step-completed.png` – completed `Medicare-Processed-Data-Validation` Spark step
- `03-emr-cluster-monitoring.png` – cluster idle/container monitoring
- `03.1-emr-step-monitoring.png` – submitted/completed/running/failed step metrics
- `04-emr-cluster-terminated.png` – final successful cluster termination

A detailed deployment record is available in:

`docs/emr_deployment.md`

---

## EMR Serverless Artifacts

The repository also contains EMR Serverless configuration files and Spark workloads prepared during development:

- `spark/emr_application_config.json`
- `spark/emr_configuration_overrides.json`
- `spark/emr_job_driver.json`
- `spark/emr_raw_join_job_driver.json`
- `spark/emr_scale_benchmark_job_driver.json`
- `spark/emr_raw_join_validation.py`
- `spark/scale_benchmark.py`

These artifacts document the originally planned Serverless deployment path. Because the account-level EMR Serverless vCPU quota remained under AWS review, the final cloud Spark validation was completed successfully using **Amazon EMR on EC2** instead.

---

## Big Data Scalability Strategy

The original CMS datasets provide a realistic multi-gigabyte workload. The integrated analytical dataset contains **9,781,673 records**.

The project also includes `spark/scale_benchmark.py`, which generates a transparent synthetic workload exceeding 100 million logical rows for Spark scalability testing.

Replicated benchmark rows are **not additional Medicare observations** and are not used to draw healthcare conclusions.

---

## Repository Structure

```text
Medicare-Provider-Analytics-Spark/
│
├── data/
│   ├── raw/
│   │   └── README.md
│   └── processed/
│       └── README.md
│
├── docs/
│   ├── architecture.md
│   ├── preprocessing.md
│   └── emr_deployment.md
│
├── images/
│   ├── README.md
│   └── aws-evidence/
│       ├── 01-emr-cluster-configuration.png
│       ├── 01.1-emr-cluster-running.png
│       ├── 02-emr-step-completed.png
│       ├── 03-emr-cluster-monitoring.png
│       ├── 03.1-emr-step-monitoring.png
│       └── 04-emr-cluster-terminated.png
│
├── notebooks/
│   ├── 01_dataset_profiling.ipynb
│   ├── 02_data_cleaning.ipynb
│   ├── 03_data_integration.ipynb
│   └── 04_medicare_analytics.ipynb
│
├── presentation/
│   └── README.md
│
├── spark/
│   ├── README.md
│   ├── emr_validation.py
│   ├── emr_raw_join_validation.py
│   ├── scale_benchmark.py
│   ├── emr_application_config.json
│   ├── emr_configuration_overrides.json
│   ├── emr_job_driver.json
│   ├── emr_raw_join_job_driver.json
│   └── emr_scale_benchmark_job_driver.json
│
├── sql/
│   ├── README.md
│   ├── create_athena_table.sql
│   ├── register_partitions.sql
│   └── athena_validation_queries.sql
│
├── README.md
└── .gitignore
```

---

## Project Status

- [x] Dataset profiling
- [x] Data preprocessing
- [x] Spark feature engineering
- [x] NPI-based integration
- [x] Join validation
- [x] Medicare analytics
- [x] Partitioned Parquet output
- [x] Amazon S3 deployment
- [x] Amazon Athena deployment
- [x] Athena validation queries
- [x] Amazon EMR on EC2 Spark execution
- [x] Successful YARN application completion
- [x] Automatic cluster termination validation
- [x] Public GitHub repository
- [ ] Final presentation/video package

---

## Author

**Judi-Ann Beckford**  
CS675 – Big Data: Management & Analytics
