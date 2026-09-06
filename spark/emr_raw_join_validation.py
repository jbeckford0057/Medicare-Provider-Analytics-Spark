from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------
# CS675 Medicare Provider Analytics
# Amazon EMR Serverless Raw Cross-Source Join Validation
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("MedicareProviderAnalyticsRawJoinValidation")
    .getOrCreate()
)

PROVIDER_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "raw/provider_services/PHY_R26_P05_V10_D24_Prov_Svc.csv"
)

ENROLLMENT_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "raw/provider_enrollment/PPEF_Enrollment_Extract_2026.04.01.csv"
)

OUTPUT_PATH = (
    "s3://medicare-provider-analytics-cs675-2026/"
    "emr/output/raw_join_validation/"
)

EXPECTED_PROVIDER_ROWS = 9781673

print("=" * 75)
print("MEDICARE PROVIDER ANALYTICS - RAW CROSS-SOURCE JOIN VALIDATION")
print("=" * 75)

# ---------------------------------------------------------
# Helper: find a column using several possible CMS names
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
# 1. Read both raw CMS datasets directly from Amazon S3
# ---------------------------------------------------------

provider_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PROVIDER_PATH)
)

enrollment_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ENROLLMENT_PATH)
)

print("Both raw CMS datasets loaded successfully from Amazon S3.")

print(f"Provider-service columns: {len(provider_raw.columns)}")
print(f"Enrollment columns: {len(enrollment_raw.columns)}")


# ---------------------------------------------------------
# 2. Resolve NPI columns
# ---------------------------------------------------------

provider_npi_col = resolve_column(
    provider_raw,
    [
        "NPI",
        "Rndrng_NPI",
        "RND RNG NPI",
        "Rendering NPI"
    ],
    "Provider NPI"
)

enrollment_npi_col = resolve_column(
    enrollment_raw,
    [
        "NPI",
        "Npi"
    ],
    "Enrollment NPI"
)


# ---------------------------------------------------------
# 3. Standardize NPI values
# ---------------------------------------------------------

provider = (
    provider_raw
    .withColumn(
        "JOIN_NPI",
        F.regexp_replace(
            F.col(provider_npi_col).cast("string"),
            r"[^0-9]",
            ""
        )
    )
    .filter(F.length("JOIN_NPI") == 10)
)

enrollment = (
    enrollment_raw
    .withColumn(
        "JOIN_NPI",
        F.regexp_replace(
            F.col(enrollment_npi_col).cast("string"),
            r"[^0-9]",
            ""
        )
    )
    .filter(F.length("JOIN_NPI") == 10)
)

print("NPI fields standardized.")


# ---------------------------------------------------------
# 4. Build one-row-per-NPI enrollment dimension
# ---------------------------------------------------------

enrollment_dimension = (
    enrollment
    .groupBy("JOIN_NPI")
    .agg(
        F.count("*").alias("ENROLLMENT_RECORD_COUNT")
    )
)

enrollment_npi_count = enrollment_dimension.count()

print(
    f"Unique enrollment NPIs: "
    f"{enrollment_npi_count:,}"
)


# ---------------------------------------------------------
# 5. Validate provider-service source row count
# ---------------------------------------------------------

provider_row_count = provider.count()

print(
    f"Valid provider-service rows: "
    f"{provider_row_count:,}"
)

print(
    f"Expected integrated provider-service rows: "
    f"{EXPECTED_PROVIDER_ROWS:,}"
)


# ---------------------------------------------------------
# 6. Perform cross-source left join on NPI
# ---------------------------------------------------------

joined = (
    provider.alias("p")
    .join(
        enrollment_dimension.alias("e"),
        F.col("p.JOIN_NPI") == F.col("e.JOIN_NPI"),
        "left"
    )
)

print("Cross-source NPI join completed.")


# ---------------------------------------------------------
# 7. Calculate join-quality metrics
# ---------------------------------------------------------

join_metrics = (
    joined
    .agg(
        F.count("*").alias("JOINED_ROW_COUNT"),

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
        ).alias("UNMATCHED_ROWS")
    )
    .collect()[0]
)

joined_row_count = int(join_metrics["JOINED_ROW_COUNT"])
matched_rows = int(join_metrics["MATCHED_ROWS"])
unmatched_rows = int(join_metrics["UNMATCHED_ROWS"])

match_rate = (
    matched_rows / joined_row_count * 100
    if joined_row_count > 0
    else 0.0
)

row_preservation_status = (
    "PASS"
    if joined_row_count == provider_row_count
    else "FAIL"
)

print("-" * 75)
print(f"Provider rows before join: {provider_row_count:,}")
print(f"Rows after left join:      {joined_row_count:,}")
print(f"Matched rows:              {matched_rows:,}")
print(f"Unmatched rows:            {unmatched_rows:,}")
print(f"Match rate:                {match_rate:.2f}%")
print(
    f"Left-join row preservation: "
    f"{row_preservation_status}"
)
print("-" * 75)


# ---------------------------------------------------------
# 8. Write compact validation evidence to Amazon S3
# ---------------------------------------------------------

result_df = spark.createDataFrame(
    [
        (
            provider_row_count,
            enrollment_npi_count,
            joined_row_count,
            matched_rows,
            unmatched_rows,
            float(match_rate),
            row_preservation_status
        )
    ],
    [
        "PROVIDER_ROWS_BEFORE_JOIN",
        "UNIQUE_ENROLLMENT_NPIS",
        "JOINED_ROW_COUNT",
        "MATCHED_ROWS",
        "UNMATCHED_ROWS",
        "MATCH_RATE_PERCENT",
        "ROW_PRESERVATION_STATUS"
    ]
)

result_df.coalesce(1).write.mode("overwrite").json(
    OUTPUT_PATH + "summary/"
)


# ---------------------------------------------------------
# 9. Write small sample of matched joined records
# ---------------------------------------------------------

joined_sample = (
    joined
    .select(
        F.col("p.JOIN_NPI").alias("NPI"),
        F.col("e.ENROLLMENT_RECORD_COUNT")
    )
    .where(
        F.col("e.ENROLLMENT_RECORD_COUNT").isNotNull()
    )
    .limit(100)
)

joined_sample.write.mode("overwrite").parquet(
    OUTPUT_PATH + "matched_sample/"
)


print("=" * 75)
print("RAW CROSS-SOURCE JOIN VALIDATION COMPLETED")
print(f"Row preservation: {row_preservation_status}")
print(f"Match rate: {match_rate:.2f}%")
print(f"Results written to: {OUTPUT_PATH}")
print("=" * 75)

spark.stop()
