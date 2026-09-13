-- ============================================================
-- CS675 Medicare Provider Analytics
-- Amazon Athena External Table Definition
-- ============================================================
--
-- Source:
-- Spark-generated Parquet master dataset stored in Amazon S3.
--
-- Partitioning:
-- The dataset is partitioned by PROVIDER_STATE.
-- In Athena, provider_state is defined as the partition column
-- rather than as a regular table column.
--
-- Database:
-- medicare_analytics
--
-- Table:
-- master_provider_services
-- ============================================================

CREATE DATABASE IF NOT EXISTS medicare_analytics;

CREATE EXTERNAL TABLE IF NOT EXISTS
medicare_analytics.master_provider_services
(
    npi BIGINT,

    provider_specialty STRING,

    hcpcs_code STRING,

    hcpcs_description STRING,

    place_of_service STRING,

    total_beneficiaries DOUBLE,

    total_services DOUBLE,

    avg_submitted_charge DOUBLE,

    avg_medicare_allowed DOUBLE,

    avg_medicare_payment DOUBLE,

    avg_standardized_payment DOUBLE,

    total_beneficiaries_capped DOUBLE,

    total_services_capped DOUBLE,

    avg_submitted_charge_capped DOUBLE,

    avg_medicare_allowed_capped DOUBLE,

    avg_medicare_payment_capped DOUBLE,

    avg_standardized_payment_capped DOUBLE,

    total_beneficiaries_capped_norm DOUBLE,

    total_services_capped_norm DOUBLE,

    avg_submitted_charge_capped_norm DOUBLE,

    avg_medicare_allowed_capped_norm DOUBLE,

    avg_medicare_payment_capped_norm DOUBLE,

    avg_standardized_payment_capped_norm DOUBLE,

    place_of_service_index DOUBLE,

    utilization_band STRING,

    enrollment_provider_type STRING,

    enrollment_state STRING,

    first_name STRING,

    middle_name STRING,

    last_name STRING,

    organization_name STRING,

    enrollment_record_count BIGINT,

    distinct_enrollment_types BIGINT,

    distinct_enrollment_states BIGINT,

    state_match STRING
)

PARTITIONED BY
(
    provider_state STRING
)

STORED AS PARQUET

LOCATION
's3://medicare-provider-analytics-cs675-2026/processed/master_provider_services/'

TBLPROPERTIES
(
    'parquet.column.index.access'='true'
);
