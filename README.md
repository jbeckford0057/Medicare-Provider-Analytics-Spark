# Medicare Provider Analytics Using Apache Spark and AWS

## CS675 – Big Data: Management & Analytics

**Project:** Medicare Provider Analytics Using Apache Spark and Cloud Storage  
**Course:** CS675 – Big Data: Management & Analytics  
**Institution:** King Graduate School  
**Student:** Judi-Ann Beckford  

---

## Project Overview

This project implements an end-to-end big data analytics pipeline using publicly available Medicare provider datasets from the Centers for Medicare & Medicaid Services (CMS).

The project integrates Medicare provider-service utilization data with Medicare Fee-for-Service provider enrollment data using the National Provider Identifier (NPI). Apache Spark is used for distributed profiling, preprocessing, feature engineering, integration, validation, and analytics.

The project was initially developed and tested in a local Spark environment using MinIO as S3-compatible object storage. The final cloud architecture uses Amazon Web Services (AWS), including Amazon S3 for cloud object storage and Amazon Athena for serverless SQL analytics. Amazon EMR Serverless workloads are also included for cloud-based Spark validation, cross-source integration, and scalability testing.

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
- Amazon EMR Serverless Spark job configuration
- Reproducible cloud deployment artifacts
- Synthetic 100M+ row scalability benchmarking

---

## Data Sources

Two public CMS datasets are used.

### 1. Medicare Physician & Other Practitioners – by Provider and Service

This dataset contains Medicare utilization, beneficiary, service, charge, allowed-payment, and Medicare-payment information at the provider/service level.

The source dataset is approximately 3 GB and contains millions of provider-service records.

Key fields used in the project include:

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

Because an NPI can occur more than once in the enrollment source, the enrollment data is first aggregated into a **one-row-per-NPI enrollment dimension** before joining it to the provider-service dataset. This prevents unintended row multiplication during integration.

---

## Research Questions

The analytical workflow addresses the following questions:

1. Which provider specialties account for the highest estimated Medicare payments?
2. Which HCPCS procedure codes account for the highest estimated Medicare payments?
3. Which states have the highest Medicare service utilization and estimated payment activity?
4. Which enrollment provider types account for the highest service utilization?
5. Which provider records may warrant additional analytical review based on unusually high estimated payment activity?

The project does **not** classify providers as fraudulent. High-payment or high-utilization records are treated only as candidates for further analytical review.

---

## Data Integration Strategy

The two CMS sources are integrated using the **National Provider Identifier (NPI)**.

The provider-service dataset contains multiple service-level records for an individual NPI. The enrollment source may also contain multiple enrollment records for the same NPI.

To avoid a many-to-many join, the enrollment source is transformed into a one-row-per-NPI dimension before integration.

The final integration uses a **left outer join** from the provider-service dataset to the enrollment dimension.

Local Spark validation produced:

- Final integrated rows: **9,781,673**
- Enrollment dimension: **2,556,656 unique NPIs**
- Matched provider-service rows: **9,645,673**
- Unmatched provider-service rows: **136,000**
- Match rate: **98.61%**
- Join row preservation: **PASS**

The local Spark physical execution plan used a **SortMergeJoin**. A broadcast strategy was evaluated during development but was not appropriate for the available local memory, so the scalable sort-merge strategy was retained.
---

## System Architecture

The final project architecture combines local development with AWS cloud analytics.

### Local Development Layer

The local environment was used to develop, test, debug, and validate the Spark pipeline before cloud execution.

Technologies used include:

- Python
- PySpark
- Apache Spark
- JupyterLab
- MinIO for local S3-compatible object-storage testing
- Git and GitHub for version control

MinIO served as a **local development and testing environment** and is not presented as a replacement for the final AWS architecture.

### AWS Cloud Layer

The cloud implementation uses:

- **Amazon S3** – raw, processed, analytical, EMR, and query-result storage
- **Amazon Athena** – serverless SQL querying of the processed Parquet dataset
- **Amazon EMR Serverless** – cloud Spark validation, cross-source integration, and scalability workloads
- **AWS IAM** – controlled access and EMR Serverless execution-role permissions
- **AWS Service Quotas** – EMR Serverless vCPU quota management

The primary AWS region used for the project is:

`us-east-1`

### Data Flow

The overall pipeline follows this sequence:

`CMS Raw Data → Apache Spark → Cleaning & Feature Engineering → NPI Integration → Parquet → Amazon S3 → Amazon Athena / Amazon EMR Serverless → Analytical Results`

---

## Apache Spark Processing Pipeline

The project is organized into four primary Jupyter notebooks.

### 1. Dataset Profiling

`notebooks/01_dataset_profiling.ipynb`

The profiling stage examines the structure and quality of the source datasets before transformation.

Activities include:

- Schema inspection
- Row and column analysis
- Missing-value assessment
- Descriptive statistics
- Identification of relevant integration fields
- Initial data-quality review

### 2. Data Cleaning and Feature Engineering

`notebooks/02_data_cleaning.ipynb`

The cleaning pipeline prepares the Medicare provider-service data for integration and analysis.

Processing includes:

- Data-type standardization
- Missing-value treatment
- Numeric quality checks
- Outlier treatment
- Capped numeric measures
- Normalized analytical features
- Categorical feature preparation
- Utilization-band creation

Original analytical values are retained where required so that capped and normalized features do not replace the underlying Medicare measures used in final interpretation.

### 3. Cross-Source Data Integration

`notebooks/03_data_integration.ipynb`

The integration pipeline:

1. Loads the cleaned provider-service dataset.
2. Loads the Medicare provider-enrollment dataset.
3. Standardizes the NPI integration key.
4. Aggregates enrollment records into a one-row-per-NPI dimension.
5. Performs a left outer join on NPI.
6. Validates row preservation.
7. Calculates matched and unmatched records.
8. Evaluates state consistency between the two sources.
9. Writes the integrated master dataset as Parquet.

The final master dataset contains **9,781,673 rows** and is partitioned by `PROVIDER_STATE`.

### 4. Medicare Analytics

`notebooks/04_medicare_analytics.ipynb`

The analytical notebook evaluates provider specialties, HCPCS procedures, geographic utilization, enrollment provider types, and high-payment records.

An analytical estimate is calculated as:

`ESTIMATED_TOTAL_MEDICARE_PAYMENT = TOTAL_SERVICES × AVG_MEDICARE_PAYMENT`

This value is an **analytical estimate** and should not be interpreted as an official CMS total-payment field.

---

## Amazon S3 Implementation

The AWS deployment stores project data in the following S3 bucket:

`medicare-provider-analytics-cs675-2026`

The bucket is organized into logical prefixes:

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
---

## Amazon EMR Serverless

Amazon EMR Serverless is used as the AWS-native distributed Spark compute layer for the final cloud implementation.

The repository contains three EMR Serverless Spark workloads.

### 1. Processed Dataset Validation

`spark/emr_validation.py`

This job reads the integrated Parquet master dataset directly from Amazon S3 and validates key deployment metrics, including:

- Total integrated row count
- Distinct provider states/territories
- Connecticut row count
- Connecticut total services
- State-level utilization summaries

The corresponding job configuration is:

`spark/emr_job_driver.json`

### 2. Raw Cross-Source Join Validation

`spark/emr_raw_join_validation.py`

This workload reads both original CMS source datasets directly from Amazon S3 and reproduces the core cross-source NPI integration in the AWS environment.

The job:

- Reads the raw provider-service CSV
- Reads the raw provider-enrollment CSV
- Standardizes NPI
- Creates a one-row-per-NPI enrollment dimension
- Performs a left outer join
- Measures matched and unmatched records
- Calculates the match rate
- Tests provider-row preservation
- Writes compact validation evidence to Amazon S3

The corresponding configuration is:

`spark/emr_raw_join_job_driver.json`

### 3. 100M+ Row Scalability Benchmark

`spark/scale_benchmark.py`

A separate scalability workload is included to evaluate Spark at a substantially larger logical data volume.

The benchmark uses the real CMS-derived schemas and records as its foundation and creates synthetic replicated benchmark rows.

The replication is explicitly identified as synthetic:

**Replicated benchmark rows are not additional Medicare observations.**

The benchmark is designed so that:

- The provider-side logical dataset exceeds 100 million rows.
- The enrollment-side logical dataset exceeds 100 million rows.
- A synthetic `SCALE_COPY_ID` is added.
- The distributed join uses NPI together with the synthetic copy identifier.
- Spark performs the large-scale transformation and aggregation.
- Only compact benchmark metrics are persisted to S3 rather than writing hundreds of millions of replicated rows.

The corresponding EMR configuration is:

`spark/emr_scale_benchmark_job_driver.json`

This approach provides a transparent scalability test without misrepresenting synthetic replicated records as additional CMS data.

---

## EMR Serverless Deployment Status

The EMR Serverless application and workload definitions are included in the repository and the required AWS IAM execution-role infrastructure has been prepared.

At the time of this README revision, the AWS account-level **Max concurrent vCPUs per account** quota increase required for EMR Serverless execution is pending AWS approval.

Therefore, the repository distinguishes between:

- **Completed AWS deployment:** Amazon S3 and Amazon Athena
- **Prepared for execution:** Amazon EMR Serverless Spark workloads
- **Local development/testing:** Apache Spark, JupyterLab, and MinIO

The README should be updated with final EMR execution results after the AWS quota request is approved and the jobs are successfully executed.

---

## EMR Application Configuration

The EMR Serverless application definition is stored in:

`spark/emr_application_config.json`

The configuration includes:

- Amazon EMR release: `emr-7.13.0`
- Workload type: Spark
- Automatic application startup
- Automatic shutdown after inactivity
- Maximum application capacity: **8 vCPU**
- Maximum application memory: **32 GB**
- Idle auto-stop timeout: **5 minutes**

The capacity limit and automatic shutdown settings are used to control cloud-resource consumption and project cost.

Monitoring output is configured to use Amazon S3 through:

`spark/emr_configuration_overrides.json`

---

## Big Data Scalability Strategy

The original CMS datasets already provide a realistic multi-gigabyte analytical workload.

The raw source files stored in Amazon S3 include approximately:

- Provider-service data: **3.0 GiB**
- Provider-enrollment data: **306 MiB**

The integrated analytical dataset contains:

**9,781,673 records**

To evaluate Spark beyond the original source size, the project also contains a synthetic scalability benchmark that generates more than 100 million logical rows on each side of a join.

The benchmark is intentionally separate from the primary analytical results.

No conclusions about Medicare utilization, providers, payments, or beneficiaries are drawn from replicated benchmark rows.

The scale test exists only to evaluate distributed-processing behavior at a larger workload size.

---

## Repository Structure

```text
Medicare-Provider-Analytics-Spark/
│
├── notebooks/
│   ├── 01_dataset_profiling.ipynb
│   ├── 02_data_cleaning.ipynb
│   ├── 03_data_integration.ipynb
│   └── 04_medicare_analytics.ipynb
│
├── spark/
│   ├── emr_validation.py
│   ├── emr_raw_join_validation.py
│   ├── scale_benchmark.py
│   ├── emr_application_config.json
│   ├── emr_job_driver.json
│   ├── emr_raw_join_job_driver.json
│   ├── emr_scale_benchmark_job_driver.json
│   └── emr_configuration_overrides.json
│
├── sql/
│   ├── create_athena_table.sql
│   ├── register_partitions.sql
│   └── athena_validation_queries.sql
│
├── data/
│   ├── raw/
│   └── processed/
│
├── README.md
└── .gitignore
