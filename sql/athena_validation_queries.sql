-- ============================================================
-- CS675 Medicare Provider Analytics
-- Amazon Athena Validation and Query Optimization Tests
-- ============================================================
--
-- Database: medicare_analytics
-- Table:    master_provider_services
--
-- Purpose:
--   1. Validate the integrated dataset row count.
--   2. Demonstrate partition pruning for Connecticut.
--   3. Compare a state-filtered workload with a nationwide
--      aggregation across all registered state partitions.
--
-- NOTE:
-- Athena execution time and bytes scanned are recorded by
-- the Athena service and are not returned by the SQL itself.
-- The observed results from the final AWS deployment are
-- documented below each query.
-- ============================================================


-- ============================================================
-- QUERY 1
-- FULL DATASET ROW-COUNT VALIDATION
-- ============================================================
--
-- Purpose:
-- Confirm that the Parquet dataset deployed to Amazon S3
-- contains the same number of integrated records produced
-- by the Apache Spark pipeline.
--
-- Observed result:
--   row_count = 9,781,673
--
-- This matches the final Spark master dataset row count.
-- ============================================================

SELECT
    COUNT(*) AS row_count
FROM medicare_analytics.master_provider_services;



-- ============================================================
-- QUERY 2
-- CONNECTICUT PARTITION-PRUNING TEST
-- ============================================================
--
-- Purpose:
-- Demonstrate Athena partition pruning by filtering directly
-- on the provider_state partition column.
--
-- Observed result:
--   provider_state = CT
--   row_count      = 119,037
--   total_services = 22,084,376.5
--
-- Observed Athena execution evidence:
--   Data scanned = 205.32 KB
--   Runtime      = 652 ms
--
-- Because provider_state is the partition key, Athena can
-- avoid scanning unrelated state partitions.
-- ============================================================

SELECT
    provider_state,
    COUNT(*) AS row_count,
    SUM(total_services) AS total_services
FROM medicare_analytics.master_provider_services
WHERE provider_state = 'CT'
GROUP BY provider_state;



-- ============================================================
-- QUERY 3
-- NATIONWIDE STATE AGGREGATION
-- ============================================================
--
-- Purpose:
-- Execute a comparable aggregation without restricting the
-- query to one provider_state partition.
--
-- Observed result:
--   62 state/territory partition rows returned
--
-- Observed Athena execution evidence:
--   Data scanned = 16.04 MB
--   Runtime      = 1.248 seconds
--
-- Selected observed results included:
--   FL: approximately 302.17 million services
--   CA: approximately 278.06 million services
--   TX: approximately 214.35 million services
--   NY: approximately 166.62 million services
--
-- Compared with the Connecticut-filtered query, the
-- Connecticut workload reduced bytes scanned by
-- approximately 98.75%.
--
-- The primary optimization evidence is the reduction in
-- bytes scanned. Runtime can vary between executions.
-- ============================================================

SELECT
    provider_state,
    COUNT(*) AS row_count,
    SUM(total_services) AS total_services
FROM medicare_analytics.master_provider_services
GROUP BY provider_state
ORDER BY total_services DESC;



-- ============================================================
-- OPTIONAL QUERY 4
-- VERIFY REGISTERED PARTITIONS
-- ============================================================
--
-- Purpose:
-- Display the state/territory partitions currently registered
-- with the Athena table.
--
-- The deployed table contains 62 registered partitions.
-- ============================================================

SHOW PARTITIONS medicare_analytics.master_provider_services;


-- ============================================================
-- PERFORMANCE SUMMARY
-- ============================================================
--
-- Nationwide aggregation:
--   Data scanned = 16.04 MB
--
-- Connecticut partition-pruned aggregation:
--   Data scanned = 205.32 KB
--
-- Approximate reduction in bytes scanned = 98.75%
--
-- This demonstrates the benefit of storing the integrated
-- Medicare dataset as Parquet and partitioning it by
-- PROVIDER_STATE for geographically filtered analytical
-- workloads.
-- ============================================================
