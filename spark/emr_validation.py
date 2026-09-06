from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------
# CS675 Medicare Provider Analytics
# Amazon EMR Serverless Cloud Validation Job
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("MedicareProviderAnalyticsEMRValidation")
    .getOrCreate()
)

INPUT_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "processed/master_provider_services/"
)

OUTPUT_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "emr/output/validation/"
)

EXPECTED_ROW_COUNT = 9781673

print("=" * 70)
print("MEDICARE PROVIDER ANALYTICS - EMR SERVERLESS VALIDATION")
print("=" * 70)

# Read the integrated Spark dataset from Amazon S3
df = spark.read.parquet(INPUT_PATH)

print("Dataset loaded successfully from Amazon S3.")

# ---------------------------------------------------------
# 1. Validate total row count
# ---------------------------------------------------------

total_rows = df.count()

print(f"Total rows: {total_rows:,}")
print(f"Expected rows: {EXPECTED_ROW_COUNT:,}")

if total_rows == EXPECTED_ROW_COUNT:
    validation_status = "PASS"
else:
    validation_status = "FAIL"

print(f"Row-count validation: {validation_status}")

# ---------------------------------------------------------
# 2. Validate partition/state coverage
# ---------------------------------------------------------

state_count = (
    df.select("PROVIDER_STATE")
    .where(F.col("PROVIDER_STATE").isNotNull())
    .distinct()
    .count()
)

print(f"Distinct provider states/territories: {state_count}")

# ---------------------------------------------------------
# 3. Connecticut partition validation
# ---------------------------------------------------------

ct_df = df.filter(F.col("PROVIDER_STATE") == "CT")

ct_metrics = (
    ct_df.agg(
        F.count("*").alias("ct_row_count"),
        F.sum("TOTAL_SERVICES").alias("ct_total_services")
    )
    .collect()[0]
)

ct_row_count = ct_metrics["ct_row_count"]
ct_total_services = ct_metrics["ct_total_services"]

print(f"Connecticut rows: {ct_row_count:,}")
print(f"Connecticut total services: {ct_total_services:,.1f}")

# ---------------------------------------------------------
# 4. Generate state-level Spark aggregation
# ---------------------------------------------------------

state_summary = (
    df.groupBy("PROVIDER_STATE")
    .agg(
        F.count("*").alias("ROW_COUNT"),
        F.sum("TOTAL_SERVICES").alias("TOTAL_SERVICES")
    )
    .orderBy(F.desc("TOTAL_SERVICES"))
)

print("Top 10 states by total services:")

state_summary.show(10, truncate=False)

# ---------------------------------------------------------
# 5. Write cloud validation output back to Amazon S3
# ---------------------------------------------------------

validation_result = spark.createDataFrame(
    [
        (
            validation_status,
            total_rows,
            EXPECTED_ROW_COUNT,
            state_count,
            ct_row_count,
            float(ct_total_services)
        )
    ],
    [
        "VALIDATION_STATUS",
        "ACTUAL_ROW_COUNT",
        "EXPECTED_ROW_COUNT",
        "DISTINCT_PROVIDER_STATES",
        "CT_ROW_COUNT",
        "CT_TOTAL_SERVICES"
    ]
)

validation_result.coalesce(1).write.mode("overwrite").json(
    OUTPUT_PATH + "summary/"
)

state_summary.write.mode("overwrite").parquet(
    OUTPUT_PATH + "state_summary/"
)

print("=" * 70)
print("EMR SERVERLESS VALIDATION COMPLETED")
print(f"Validation status: {validation_status}")
print(f"Results written to: {OUTPUT_PATH}")
print("=" * 70)

spark.stop()
