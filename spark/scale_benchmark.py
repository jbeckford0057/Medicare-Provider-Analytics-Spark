from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from math import ceil
import time

# ---------------------------------------------------------
# CS675 Medicare Provider Analytics
# Synthetic Big-Data Scalability Benchmark
#
# IMPORTANT:
# This benchmark creates synthetic replicated rows from
# real CMS datasets solely to test Spark scalability.
# Replicated rows are NOT additional Medicare observations.
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("MedicareProviderAnalyticsScaleBenchmark")
    .getOrCreate()
)

PROVIDER_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "processed/master_provider_services/"
)

ENROLLMENT_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "raw/provider_enrollment/PPEF_Enrollment_Extract_2026.04.01.csv"
)

OUTPUT_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "emr/output/scale_benchmark/"
)

TARGET_ROWS = 100_000_000

print("=" * 78)
print("MEDICARE PROVIDER ANALYTICS - SYNTHETIC SCALE BENCHMARK")
print("=" * 78)
print(
    "NOTICE: Replicated rows are synthetic scalability-test records "
    "derived from real CMS data."
)


# ---------------------------------------------------------
# Helper function to locate NPI column
# ---------------------------------------------------------

def resolve_column(df, candidates, label):
    lookup = {c.lower().strip(): c for c in df.columns}

    for candidate in candidates:
        key = candidate.lower().strip()

        if key in lookup:
            actual = lookup[key]
            print(f"{label}: using source column '{actual}'")
            return actual

    raise ValueError(
        f"Could not locate {label}. "
        f"Available columns: {df.columns}"
    )


# ---------------------------------------------------------
# 1. Read real CMS provider-service master dataset
# ---------------------------------------------------------

provider_base = (
    spark.read
    .parquet(PROVIDER_PATH)
    .select(
        F.col("NPI").cast("string").alias("JOIN_NPI"),
        F.col("PROVIDER_STATE"),
        F.col("TOTAL_SERVICES")
    )
    .filter(F.col("JOIN_NPI").isNotNull())
)

provider_base_count = provider_base.count()

print(f"Real provider-service rows: {provider_base_count:,}")


# ---------------------------------------------------------
# 2. Read real CMS enrollment dataset
# ---------------------------------------------------------

enrollment_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ENROLLMENT_PATH)
)

enrollment_npi_col = resolve_column(
    enrollment_raw,
    ["NPI", "Npi"],
    "Enrollment NPI"
)

enrollment_base = (
    enrollment_raw
    .select(
        F.regexp_replace(
            F.col(enrollment_npi_col).cast("string"),
            r"[^0-9]",
            ""
        ).alias("JOIN_NPI")
    )
    .filter(F.length("JOIN_NPI") == 10)
    .groupBy("JOIN_NPI")
    .agg(
        F.count("*").alias("ENROLLMENT_RECORD_COUNT")
    )
)

enrollment_base_count = enrollment_base.count()

print(f"Real unique enrollment NPIs: {enrollment_base_count:,}")


# ---------------------------------------------------------
# 3. Determine copies required to exceed 100M rows
# ---------------------------------------------------------

provider_copies = ceil(
    TARGET_ROWS / provider_base_count
)

enrollment_copies = ceil(
    TARGET_ROWS / enrollment_base_count
)

print(f"Provider replication factor: {provider_copies}")
print(f"Enrollment replication factor: {enrollment_copies}")


# ---------------------------------------------------------
# 4. Create synthetic scaled provider dataset
# ---------------------------------------------------------

provider_copy_ids = (
    spark.range(provider_copies)
    .select(
        F.col("id").cast("int").alias("SCALE_COPY_ID")
    )
)

provider_scaled = (
    provider_base
    .crossJoin(F.broadcast(provider_copy_ids))
)

provider_scaled_count = (
    provider_base_count * provider_copies
)

print(
    f"Synthetic provider logical rows: "
    f"{provider_scaled_count:,}"
)


# ---------------------------------------------------------
# 5. Create synthetic scaled enrollment dataset
# ---------------------------------------------------------

enrollment_copy_ids = (
    spark.range(enrollment_copies)
    .select(
        F.col("id").cast("int").alias("SCALE_COPY_ID")
    )
)

enrollment_scaled = (
    enrollment_base
    .crossJoin(F.broadcast(enrollment_copy_ids))
)

enrollment_scaled_count = (
    enrollment_base_count * enrollment_copies
)

print(
    f"Synthetic enrollment logical rows: "
    f"{enrollment_scaled_count:,}"
)


# ---------------------------------------------------------
# 6. Verify benchmark scale requirements
# ---------------------------------------------------------

provider_scale_status = (
    "PASS"
    if provider_scaled_count >= TARGET_ROWS
    else "FAIL"
)

enrollment_scale_status = (
    "PASS"
    if enrollment_scaled_count >= TARGET_ROWS
    else "FAIL"
)

print(
    f"Provider 100M+ requirement: "
    f"{provider_scale_status}"
)

print(
    f"Enrollment 100M+ requirement: "
    f"{enrollment_scale_status}"
)


# ---------------------------------------------------------
# 7. Execute distributed cross-source join benchmark
#
# Join key:
#   JOIN_NPI + SCALE_COPY_ID
#
# Both benchmark inputs exceed 100M logical rows.
# ---------------------------------------------------------

benchmark_start = time.time()

joined_scaled = (
    provider_scaled.alias("p")
    .join(
        enrollment_scaled.alias("e"),
        on=[
            F.col("p.JOIN_NPI") == F.col("e.JOIN_NPI"),
            F.col("p.SCALE_COPY_ID")
            == F.col("e.SCALE_COPY_ID")
        ],
        how="left"
    )
)


# ---------------------------------------------------------
# 8. Trigger Spark execution and collect aggregate metrics
# ---------------------------------------------------------

benchmark_metrics = (
    joined_scaled
    .agg(
        F.count("*").alias("JOINED_ROWS"),

        F.sum(
            F.when(
                F.col("e.ENROLLMENT_RECORD_COUNT").isNotNull(),
                1
            ).otherwise(0)
        ).alias("MATCHED_ROWS"),

        F.sum(
            F.when(
                F.col("e.ENROLLMENT_RECORD_COUNT").isNull(),
                1
            ).otherwise(0)
        ).alias("UNMATCHED_ROWS"),

        F.sum(
            F.col("p.TOTAL_SERVICES")
        ).alias("TOTAL_SERVICES")
    )
    .collect()[0]
)

benchmark_end = time.time()

elapsed_seconds = (
    benchmark_end - benchmark_start
)

joined_rows = int(
    benchmark_metrics["JOINED_ROWS"]
)

matched_rows = int(
    benchmark_metrics["MATCHED_ROWS"]
)

unmatched_rows = int(
    benchmark_metrics["UNMATCHED_ROWS"]
)

match_rate = (
    matched_rows / joined_rows * 100
    if joined_rows > 0
    else 0.0
)

row_preservation_status = (
    "PASS"
    if joined_rows == provider_scaled_count
    else "FAIL"
)


# ---------------------------------------------------------
# 9. Display benchmark results
# ---------------------------------------------------------

print("-" * 78)

print(
    f"Provider benchmark rows:   "
    f"{provider_scaled_count:,}"
)

print(
    f"Enrollment benchmark rows: "
    f"{enrollment_scaled_count:,}"
)

print(
    f"Joined output rows:        "
    f"{joined_rows:,}"
)

print(
    f"Matched rows:              "
    f"{matched_rows:,}"
)

print(
    f"Unmatched rows:            "
    f"{unmatched_rows:,}"
)

print(
    f"Match rate:                "
    f"{match_rate:.2f}%"
)

print(
    f"Row preservation:          "
    f"{row_preservation_status}"
)

print(
    f"Benchmark runtime:         "
    f"{elapsed_seconds:.2f} seconds"
)

print("-" * 78)


# ---------------------------------------------------------
# 10. Write only compact benchmark evidence to S3
#
# We intentionally do NOT persist the 100M+ synthetic
# datasets because they exist solely as a scalability test.
# ---------------------------------------------------------

benchmark_summary = spark.createDataFrame(
    [
        (
            provider_base_count,
            provider_copies,
            provider_scaled_count,
            enrollment_base_count,
            enrollment_copies,
            enrollment_scaled_count,
            joined_rows,
            matched_rows,
            unmatched_rows,
            float(match_rate),
            float(elapsed_seconds),
            provider_scale_status,
            enrollment_scale_status,
            row_preservation_status,
            "Synthetic replication benchmark derived from real CMS data"
        )
    ],
    [
        "REAL_PROVIDER_ROWS",
        "PROVIDER_REPLICATION_FACTOR",
        "SYNTHETIC_PROVIDER_ROWS",
        "REAL_ENROLLMENT_NPIS",
        "ENROLLMENT_REPLICATION_FACTOR",
        "SYNTHETIC_ENROLLMENT_ROWS",
        "JOINED_ROWS",
        "MATCHED_ROWS",
        "UNMATCHED_ROWS",
        "MATCH_RATE_PERCENT",
        "RUNTIME_SECONDS",
        "PROVIDER_100M_STATUS",
        "ENROLLMENT_100M_STATUS",
        "ROW_PRESERVATION_STATUS",
        "BENCHMARK_NOTE"
    ]
)

benchmark_summary.coalesce(1).write.mode(
    "overwrite"
).json(
    OUTPUT_PATH + "summary/"
)


print("=" * 78)
print("SYNTHETIC BIG-DATA SCALE BENCHMARK COMPLETED")
print(
    "The 100M+ row counts represent replicated benchmark "
    "records, not additional CMS observations."
)
print(
    f"Results written to: {OUTPUT_PATH}"
)
print("=" * 78)

spark.stop()
