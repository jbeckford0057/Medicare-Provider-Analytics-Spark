# Project Architecture

This project analyzes Medicare provider utilization, services, and enrollment data using Apache Spark.

## Local Development Architecture

CMS CSV Data
-> Apache Spark
-> Data Profiling and Cleaning
-> NPI-Based Integration
-> Partitioned Parquet Outputs
-> Local Analytics

## AWS Architecture

CMS Source Data
-> Amazon S3
-> Apache Spark / AWS Compute
-> Cleaned and Integrated Parquet Data
-> Amazon S3
-> Amazon Athena
-> Analytical Queries and Results

The AWS implementation is designed to support reproducible processing of large Medicare datasets at cloud scale.
