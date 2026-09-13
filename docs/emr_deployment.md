# Amazon EMR on EC2 Deployment Evidence

## Purpose

This document records the successful AWS-hosted Spark validation completed for the Medicare Provider Analytics project.

## Final Cluster

- Cluster name: `Medicare-Provider-Analytics-Spark`
- Amazon EMR release: `emr-7.14.0`
- Apache Spark: `3.5.8`
- Hadoop: `3.4.2`
- Capacity: `1 Primary / 0 Core / 0 Task`
- Primary instance type: `r8g.xlarge`
- Region: `us-east-1`
- Cluster logging: Amazon S3 enabled
- Automatic idle termination: 10 minutes

## IAM and Network Configuration

- EMR service role: `EMR_DefaultRole_V2`
- EC2 instance profile: `EMR_EC2_DefaultRole`
- VPC and subnet were tagged for use with Amazon EMR managed policies.

## Successful Spark Step

Step name:

`Medicare-Processed-Data-Validation`

Application:

`s3://medicare-provider-analytics-cs675-2026/emr/scripts/emr_validation.py`

Observed execution evidence:

- EMR step status: **Completed**
- YARN application ID was created successfully
- Spark application reached RUNNING state
- Controller log reported `exit code 0`
- Controller log reported total step runtime of **128 seconds**
- EMR monitoring showed the application complete and the cluster return to idle
- The cluster later terminated automatically according to the configured idle policy

## Outcome

The completed step confirms that the processed Medicare dataset and Spark validation workflow ran successfully in AWS on Amazon EMR on EC2.

The project therefore includes both local Spark development and a verified cloud Spark execution path.
