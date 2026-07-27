# Medicare Provider Analytics Using Apache Spark and AWS

## Project Overview

This project develops a scalable big data analytics solution using Apache Spark and AWS to analyze Medicare provider utilization and payment data. By integrating provider enrollment information with Medicare service records, the project aims to identify patterns in healthcare utilization, provider characteristics, and Medicare reimbursement across the United States.

---

## Project Objectives

- Develop a Spark-based data processing pipeline.
- Integrate Medicare provider datasets using the National Provider Identifier (NPI).
- Analyze provider utilization, Medicare reimbursement, and payment trends.
- Deploy the solution using AWS cloud services.
- Document the project through GitHub.

---

## Research Questions

This project seeks to answer the following questions:

1. Which provider specialties generate the highest Medicare payments?

2. Which HCPCS procedures account for the greatest Medicare spending?

3. Which states have the highest Medicare utilization?

4. Which provider types perform the greatest number of Medicare services?

---
## Datasets

### Dataset 1
**Medicare Physician & Other Practitioners by Provider and Service**

Contains provider utilization, services performed, Medicare payments, and procedure information.

### Dataset 2
**Medicare Fee-for-Service Public Provider Enrollment**

Contains provider information including specialty, provider type, enrollment status, and geographic information.

---

### Join Strategy

The two datasets will be joined using the National Provider Identifier (NPI), providing a reliable one-to-one relationship between provider information and Medicare utilization records.

---

## Technology Stack

- Python
- Apache Spark (PySpark)
- Docker
- Jupyter Notebook
- Amazon S3
- Amazon Athena
- Terraform
- GitHub

---

## Project Workflow

Raw Medicare Data
      │
      ▼
Data Preprocessing
      │
      ▼
Apache Spark
      │
      ▼
Dataset Integration (NPI)
      │
      ▼
Analytics
      │
      ▼
AWS Deployment
      │
      ▼
Results

---

## Project Timeline

- July 25 – Project planning and scope review
- July 28–30 – Data profiling, preprocessing, and Spark analytics
- July 31 – AWS deployment and testing
- August 1 – Final project presentation

---

## Repository Structure

## Repository Structure

```text
medicare-provider-analytics-spark/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docs/
├── images/
├── notebooks/
├── presentation/
├── spark/
├── sql/
├── terraform/
└── README.md
```

---

## Expected Deliverables

- Apache Spark data preprocessing pipeline
- Integrated Medicare datasets using NPI
- Spark analytical queries
- AWS cloud deployment
- Public GitHub repository
- Final presentation and demonstration

---

## Current Status

- [x] Project selected
- [x] Datasets identified
- [ ] Dataset profiling
- [ ] Data preprocessing
- [ ] Spark joins
- [ ] Analytics
- [ ] AWS deployment
- [ ] Final presentation

----

## Author

**Judi-Ann Beckford**

CS-675 Big Data Management & Analytics

Spring Semester 2026
