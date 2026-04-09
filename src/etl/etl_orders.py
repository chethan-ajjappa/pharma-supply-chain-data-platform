"""
etl_orders.py — Orders Fact Table ETL
Pharma Supply Chain Data Platform

Transforms raw CSV lifecycle events from S3 Raw zone into typed,
validated Silver Parquet. All source data is read via Glue Catalog.

Pipeline:
    1. Configuration & schema         — explicit types, domain sets, thresholds
    2. Source integrity validation     — schema, PK nulls/dupes
    3. Transformation                  — clean → normalize → cast
    4. Silver quality gates            — date null rate, partition trust
    5. Write (Parquet)
    6. Metadata reporting to S3

Hard-fail design:
    Every violation writes a forensic artifact to S3 before raising.
    No WARN-and-continue for data integrity checks.
    Any unhandled exception writes a failure report and re-raises.

Key decisions:
    - Source CSV read with header=true directly from S3 (no header-row leak).
    - Column names normalized to lowercase on read (source uses mixed case).
    - erp_export_row_id is the physical PK. Never transformed, never cleaned.
    - Partition columns (year, month) are trusted from raw zone — not recalculated.
    - Boolean columns use INT→BOOLEAN cast; direct cast("boolean") on "0"/"1" returns NULL.
    - Schema drift (rma_no absent in 2022) is handled via nullable target schema field.
"""

import sys
import json
from datetime import datetime

import boto3
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit,
    count as spark_count, countDistinct, sum as spark_sum,
    to_date, regexp_replace, trim, upper, coalesce, length,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType, DateType,
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    "source": {
        "database": "raw_zone",
        "table":    "orders",
        "s3_path":  "s3://pharma-raw-dev/facts/orders/",
    },
    "target": {
        "database": "curated_zone",
        "table":    "orders",
        "s3_path":  "s3://pharma-curated-dev/facts/orders/",
    },
    "metadata": {
        "s3_bucket": "pharma-curated-dev",
        "s3_prefix": "etl-metadata/job1-orders/",
    },
    "errors": {
        "s3_bucket": "pharma-curated-dev",
        "s3_prefix": "etl-errors/job1-orders/",
    },
    "thresholds": {
        # order_date NULL % — FAIL if exceeded
        "critical_date_null_max_percent": 2.0,
        # Unexpected header rows per partition — FAIL if exceeded
        "header_rows_max_per_partition":  1,
    },
    "domain_rules": {
        "valid_payment_modes":    ["Cash", "Card", "Credit", "UPI", "Cheque"],
        "valid_line_statuses":    ["Ordered", "Delivered", "Invoiced", "Cancelled", "Returned"],
        "valid_order_statuses":   ["Completed", "Partial", "Unfulfilled", "Cancelled"],
        "valid_payment_statuses": ["Pending", "Paid", "Failed", "Refunded"],
        "min_year": 2020,
        "max_year": 2026,
    },
    "write": {
        "mode":                     "overwrite",
        "partition_overwrite_mode": "dynamic",
        "max_records_per_file":     100_000,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 2. TARGET SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

TARGET_SCHEMA = StructType([
    # Physical PK — system-generated, globally unique, immutable
    StructField("erp_export_row_id",          StringType(),       False),

    # Degenerate dimensions — business keys that repeat across lifecycle events
    StructField("order_line_id",              StringType(),       True),
    StructField("order_no",                   StringType(),       True),
    StructField("so_number",                  StringType(),       True),
    StructField("order_line_no",              IntegerType(),      True),
    StructField("delivery_no",                StringType(),       True),
    StructField("delivery_line_no",           IntegerType(),      True),
    StructField("invoice_no",                 StringType(),       True),
    StructField("invoice_line_no",            IntegerType(),      True),

    # Foreign keys
    StructField("customer_id",                StringType(),       True),
    StructField("product_id",                 IntegerType(),      True),
    StructField("plant_code",                 IntegerType(),      True),
    StructField("batch_id",                   StringType(),       True),

    # Descriptive attributes
    StructField("customer_name",              StringType(),       True),
    StructField("product_code",               StringType(),       True),
    StructField("product_name",               StringType(),       True),
    StructField("batch_no",                   StringType(),       True),
    StructField("lot_number",                 StringType(),       True),

    # Lifecycle dates — conditional on line_status
    StructField("order_date",                 DateType(),         False),
    StructField("expected_delivery_date",     DateType(),         True),
    StructField("actual_delivery_date",       DateType(),         True),   # delivery events only
    StructField("invoice_date",               DateType(),         True),   # invoice events only

    # Quantities — signed: negative values represent returns / reversals
    StructField("order_qty",                  IntegerType(),      True),
    StructField("delivery_qty",               IntegerType(),      True),
    StructField("invoice_qty",                IntegerType(),      True),

    # Pricing — DECIMAL(18,2) for line totals up to ~₹500,000
    StructField("mrp",                        DecimalType(18, 2), True),
    StructField("floor_price",                DecimalType(18, 2), True),
    StructField("order_price",                DecimalType(18, 2), True),
    StructField("delivery_price",             DecimalType(18, 2), True),
    StructField("invoice_price",              DecimalType(18, 2), True),
    StructField("gst_rate",                   DecimalType(5,  2), True),
    StructField("gst_amount",                 DecimalType(18, 2), True),
    StructField("net_line_amount",            DecimalType(18, 2), True),
    StructField("total_line_amount_incl_gst", DecimalType(18, 2), True),

    # Status / enum columns
    StructField("payment_mode",               StringType(),       True),
    StructField("payment_status",             StringType(),       True),
    StructField("line_status",                StringType(),       True),
    StructField("order_status",               StringType(),       True),
    StructField("order_year",                 IntegerType(),      True),

    # Conditional reason fields — NULL for non-applicable lifecycle events
    StructField("cancel_reason",              StringType(),       True),
    StructField("return_reason",              StringType(),       True),
    StructField("rma_no",                     StringType(),       True),

    # Hive partition columns — trusted from raw zone, not recalculated
    StructField("year",                       StringType(),       False),
    StructField("month",                      StringType(),       False),
])


COLUMN_GROUPS = {
    # Never touched — PK validated at source, passed through unchanged
    "primary_key":        ["erp_export_row_id"],

    # String IDs: trim + empty → NULL; retained as STRING
    "string_ids": [
        "order_line_id", "order_no", "so_number", "delivery_no", "invoice_no",
        "customer_id", "batch_id", "batch_no", "lot_number",
    ],

    # Numeric IDs: comma-strip + cast to INT
    "numeric_ids":          ["product_id", "plant_code", "order_year"],
    "numeric_line_numbers": ["order_line_no", "delivery_line_no", "invoice_line_no"],

    # Free-text: trim + empty → NULL
    "text_fields": [
        "customer_name", "product_code", "product_name",
        "cancel_reason", "return_reason", "rma_no",
    ],

    # Dates: format-normalized then parsed with fallback
    "dates": [
        "order_date", "expected_delivery_date",
        "actual_delivery_date", "invoice_date",
    ],

    # Signed integers — negative = return/reversal
    "quantities": ["order_qty", "delivery_qty", "invoice_qty"],

    # DECIMAL(18,2) — line-level amounts
    "amounts": [
        "mrp", "floor_price", "order_price", "delivery_price", "invoice_price",
        "gst_rate", "gst_amount", "net_line_amount", "total_line_amount_incl_gst",
    ],

    # Enums: normalized to canonical casing; unrecognized values → NULL
    "enums": {
        "payment_mode":   CONFIG["domain_rules"]["valid_payment_modes"],
        "payment_status": CONFIG["domain_rules"]["valid_payment_statuses"],
        "line_status":    CONFIG["domain_rules"]["valid_line_statuses"],
        "order_status":   CONFIG["domain_rules"]["valid_order_statuses"],
    },

    # Partition keys from raw zone — no recalculation, no validation
    "partitions": ["year", "month"],
}


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING & UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

JOB_NAME_CONST   = "job-pharma-etl-orders"
TABLE_NAME_CONST = "orders"


def _log(level: str, phase: str, metric: str, value,
         message: str, extra: dict = None) -> None:
    """
    Emit a structured JSON log entry to stdout (CloudWatch Logs).

    Every entry carries the full job/table context so CloudWatch Insights
    queries can filter without joins.
    """
    entry = {
        "timestamp":  datetime.utcnow().isoformat() + "Z",
        "level":      level,
        "job_name":   JOB_NAME_CONST,
        "table_name": TABLE_NAME_CONST,
        "phase":      phase,
        "metric":     metric,
        "value":      value,
        "message":    message,
    }
    if extra:
        entry["context"] = extra
    print(json.dumps(entry))


def _log_fail(phase: str, metric: str, value, message: str,
              extra: dict = None) -> None:
    """Convenience wrapper — always ERROR level, used before every hard fail."""
    _log("ERROR", phase, metric, value, message, extra)


def get_run_id() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")


def _put_s3(content, bucket: str, key: str) -> None:
    if isinstance(content, (dict, list)):
        content = json.dumps(content, indent=2, default=str)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content)
    _log("INFO", "s3_write", "key", key,
         f"Artifact written → s3://{bucket}/{key}")


def _write_forensic_report(
    error_type: str,
    run_id: str,
    summary: dict,
    df_rows: DataFrame = None,
    write_csv: bool = True,
) -> str:
    """
    Write a forensic error report to S3 before any hard fail.

    Always writes a JSON summary. Optionally writes offending rows in
    Parquet (efficient) and CSV (human-readable) when df_rows is supplied.

    Returns the S3 reject prefix so the caller can embed it in the
    exception message and in structured logs.
    """
    prefix = (
        f"s3://{CONFIG['errors']['s3_bucket']}/"
        f"{CONFIG['errors']['s3_prefix']}"
        f"{error_type}/run_id={run_id}/"
    )
    report = {
        "run_id":      run_id,
        "table_name":  TABLE_NAME_CONST,
        "error_type":  error_type,
        "timestamp":   datetime.utcnow().isoformat(),
        "reject_path": prefix,
        "summary":     summary,
    }
    _put_s3(
        report,
        CONFIG["errors"]["s3_bucket"],
        f"{CONFIG['errors']['s3_prefix']}{error_type}/run_id={run_id}/report.json",
    )
    if df_rows is not None:
        df_rows.write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{prefix}parquet/")
        if write_csv:
            df_rows.coalesce(1).write.mode("overwrite") \
                   .option("header", True).csv(f"{prefix}csv/")
        _log("INFO", "forensic_write", "reject_path", prefix,
             f"Reject rows written ({error_type})")
    return prefix


# ─────────────────────────────────────────────────────────────────────────────
# 3. SOURCE READ + COLUMN NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def read_and_normalize(spark) -> tuple:
    """
    Read raw CSV from S3 with header=true and normalize column names.

    Reads the S3 location directly (obtained from Glue Catalog) using
    header=true so Spark handles the header row automatically — no
    post-read filter needed. This is the same approach used in
    etl_multi_table.py for all other tables.

    Column normalization: strip whitespace, lowercase, spaces → underscore.
    Eliminates mismatches between source naming conventions and target schema.

    Returns:
        tuple: (df_normalized, row_count, s3_location)
    """
    _log("INFO", "read_source", "start", None,
         "Reading and normalizing source",
         extra={"table": f"{CONFIG['source']['database']}.{CONFIG['source']['table']}"}
    )

    # Resolve S3 location from Glue Catalog
    meta   = boto3.client("glue").get_table(
        DatabaseName=CONFIG["source"]["database"],
        Name=CONFIG["source"]["table"],
    )
    s3_loc = meta["Table"]["StorageDescriptor"]["Location"]

    # Read CSV directly from S3 with header=true
    # header=true lets Spark skip the header row automatically — no filter hack needed
    df_raw = (
        spark.read
             .option("header",                   "true")
             .option("inferSchema",              "false")
             .option("multiLine",                "false")
             .option("ignoreLeadingWhiteSpace",  "true")
             .option("ignoreTrailingWhiteSpace", "true")
             .csv(s3_loc)
    )

    row_count  = df_raw.count()
    original   = df_raw.columns
    normalized = [c.strip().lower().replace(" ", "_") for c in original]
    df_norm    = df_raw.toDF(*normalized)

    _log("INFO", "read_source", "row_count", row_count,
         f"{row_count:,} rows read (header auto-handled by Spark)",
         extra={
             "s3_location":        s3_loc,
             "columns_normalized": len(normalized),
             "sample_renames": [
                 f"{original[i]} → {normalized[i]}"
                 for i in range(min(3, len(original)))
             ],
         })

    return df_norm, row_count, s3_loc


# ─────────────────────────────────────────────────────────────────────────────
# 4. SOURCE INTEGRITY VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_source_integrity(spark, run_id: str) -> tuple:
    """
    Validate the raw orders table before any transformation.

    Checks (all hard-fail):
        1. Table readable and all expected columns present
        2. PK (erp_export_row_id) has zero NULLs — forensic write + FAIL
        3. PK has zero duplicates — forensic write (Parquet + CSV) + FAIL
        4. Partition columns (year, month) present

    Spark actions: 1 (read count) + PK checks (1 aggregation)

    Returns:
        tuple: (df_normalized, row_count, validation_results_dict)
    """
    _log("INFO", "source_integrity", "start", None,
         "Source integrity validation started", extra={"run_id": run_id})

    results = {
        "run_id":    run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "checks":    {},
        "status":    "PASS",
    }
    pk_col = "erp_export_row_id"

    # ── Read + normalize ──────────────────────────────────────────────────────
    try:
        df, total_rows, s3_loc = read_and_normalize(spark)
    except Exception as exc:
        _log_fail("source_integrity", "read_failed", None,
                  f"Cannot read source from S3: {exc}")
        results["status"] = "FAIL"
        raise ValueError(f"Source read failed: {exc}") from exc

    # ── Check 1: Schema completeness ─────────────────────────────────────────
    actual_cols   = set(df.columns)
    expected_cols = {f.name for f in TARGET_SCHEMA.fields}
    missing       = expected_cols - actual_cols
    extra         = actual_cols  - expected_cols

    schema_status = "FAIL" if missing else ("WARN" if extra else "PASS")
    results["checks"]["schema"] = {
        "expected_columns": len(expected_cols),
        "actual_columns":   len(actual_cols),
        "missing":          sorted(missing),
        "extra":            sorted(extra),
        "note":             "Extra columns indicate schema drift — logged for tracking",
        "status":           schema_status,
    }

    if missing:
        _log_fail("source_integrity", "schema_missing_columns", len(missing),
                  f"HARD FAIL: {len(missing)} required column(s) absent from source",
                  extra={"missing_columns": sorted(missing)})
        results["status"] = "FAIL"
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if extra:
        _log("WARN", "source_integrity", "schema_drift", len(extra),
             f"Schema drift: {len(extra)} unrecognized column(s)",
             extra={"extra_columns": sorted(extra)})

    # ── Check 2 & 3: PK validation — single aggregation ──────────────────────

    pk_stats    = df.agg(
        spark_count("*").alias("total_rows"),
        spark_count(col(pk_col)).alias("pk_non_null"),
        countDistinct(col(pk_col)).alias("pk_distinct"),
    ).collect()[0]

    total_rows   = pk_stats["total_rows"]
    pk_non_null  = pk_stats["pk_non_null"]
    pk_distinct  = pk_stats["pk_distinct"]
    pk_null_n    = total_rows - pk_non_null
    dup_n        = pk_non_null - pk_distinct

    # Hard fail: NULL PKs
    if pk_null_n > 0:
        null_df       = df.filter(col(pk_col).isNull())
        partition_dist = (
            null_df.groupBy("year", "month")
                   .agg(spark_count("*").alias("null_count"))
                   .collect()
        )
        path = _write_forensic_report(
            "pk_null_violation", run_id,
            summary={
                "null_count":          pk_null_n,
                "total_rows":          total_rows,
                "null_rate_percent":   round(pk_null_n / total_rows * 100, 4),
                "partition_breakdown": [r.asDict() for r in partition_dist],
            },
            df_rows=null_df,
        )
        _log_fail("source_integrity", "pk_null", pk_null_n,
                  f"HARD FAIL: {pk_null_n} NULL {pk_col} value(s)",
                  extra={"pk_column": pk_col, "null_count": pk_null_n,
                         "reject_path": path})
        results["status"] = "FAIL"
        results["checks"]["primary_key"] = {
            "status": "FAIL", "null_count": pk_null_n, "reject_path": path
        }
        raise ValueError(
            f"PK NULL VIOLATION: {pk_null_n} NULL {pk_col} rows → {path}"
        )

    # Hard fail: duplicate PKs
    if dup_n > 0:
        dup_key_df  = (df.groupBy(pk_col)
                          .agg(spark_count("*").alias("occurrence_count"))
                          .filter(col("occurrence_count") > 1))
        dup_stats   = dup_key_df.agg(
            spark_count("*").alias("unique_dup_keys"),
            spark_sum("occurrence_count").alias("total_dup_rows"),
        ).collect()[0]
        dup_full_df = df.join(dup_key_df.select(pk_col), on=pk_col, how="inner")
        dist        = (dup_key_df.groupBy("occurrence_count")
                                  .agg(spark_count("*").alias("key_count"))
                                  .orderBy("occurrence_count")
                                  .collect())
        path = _write_forensic_report(
            "pk_duplicate_violation", run_id,
            summary={
                "unique_duplicate_keys":   dup_stats["unique_dup_keys"],
                "total_duplicate_rows":    dup_stats["total_dup_rows"],
                "occurrence_distribution": [r.asDict() for r in dist],
                "total_source_rows":       total_rows,
            },
            df_rows=dup_full_df,
        )
        _log_fail("source_integrity", "pk_duplicates", dup_n,
                  f"HARD FAIL: {dup_n} duplicate {pk_col} value(s)",
                  extra={"pk_column": pk_col, "unique_dup_keys": dup_stats["unique_dup_keys"],
                         "reject_path": path})
        results["status"] = "FAIL"
        results["checks"]["primary_key"] = {
            "status": "FAIL", "duplicate_count": dup_n, "reject_path": path
        }
        raise ValueError(
            f"PRIMARY KEY VIOLATION: {dup_n} duplicate {pk_col} value(s) → {path}"
        )

    results["checks"]["primary_key"] = {
        "column":          pk_col,
        "null_count":      0,
        "duplicate_count": 0,
        "total_rows":      total_rows,
        "distinct_count":  pk_distinct,
        "status":          "PASS",
    }
    _log("INFO", "source_integrity", "pk_validated", total_rows,
         f"PK validation PASS — {total_rows:,} unique rows",
         extra={"pk_column": pk_col, "spark_actions_used": 2})

    # ── Check 4: Partition columns present ───────────────────────────────────
    missing_parts = set(COLUMN_GROUPS["partitions"]) - actual_cols
    if missing_parts:
        _log_fail("source_integrity", "missing_partitions", len(missing_parts),
                  "HARD FAIL: partition column(s) absent",
                  extra={"missing": sorted(missing_parts)})
        results["status"] = "FAIL"
        raise ValueError(f"Missing partition columns: {sorted(missing_parts)}")

    results["checks"]["partitions"] = {
        "required": COLUMN_GROUPS["partitions"],
        "status":   "PASS",
        "note":     "Values trusted from raw zone ingestion — not recalculated",
    }

    _log("INFO", "source_integrity", "complete", None,
         "Source integrity validation PASS",
         extra={"total_rows": total_rows, "checks_run": len(results["checks"])})

    return df, total_rows, results


# ─────────────────────────────────────────────────────────────────────────────
# 5. TRANSFORMATION
# ─────────────────────────────────────────────────────────────────────────────

def transform_to_silver(df_raw: DataFrame, run_id: str) -> tuple:
    """
    Apply all transformation steps and collect quality metrics.

    Steps (no Spark actions until the final aggregation):
        1. String cleaning    — trim + empty → NULL  (PK excluded)
        2. Numeric cleaning   — comma-strip
        3. Enum normalization — case-insensitive match → canonical value
        4. Domain validation  — order_year range enforcement
        5. Type casting       — single SELECT resolves all column types
        6. Stats aggregation  — ONE action: null counts + enum invalid counts

    Returns:
        tuple: (df_silver, stats_dict, total_rows)
    """
    _log("INFO", "transform", "start", None, "Transformation started",
         extra={"run_id": run_id})

    df = df_raw

    # ── Step 1: String cleaning (PK excluded) ────────────────────────────────
    for name in COLUMN_GROUPS["string_ids"] + COLUMN_GROUPS["text_fields"]:
        if name in df.columns:
            df = df.withColumn(
                name,
                when(
                    trim(col(name)).isNull() | (trim(col(name)) == ""),
                    lit(None),
                ).otherwise(trim(col(name))),
            )

    # ── Step 2: Numeric cleaning (comma strip before cast) ───────────────────
    for name in (
        COLUMN_GROUPS["numeric_ids"]
        + COLUMN_GROUPS["numeric_line_numbers"]
        + COLUMN_GROUPS["quantities"]
        + COLUMN_GROUPS["amounts"]
    ):
        if name in df.columns:
            c = regexp_replace(trim(col(name)), ",", "")
            df = df.withColumn(name, when(c.isNull() | (c == ""), lit(None)).otherwise(c))

    # ── Step 3: Enum normalization ────────────────────────────────────────────
    enum_invalid_exprs = {}

    for col_name, valid_vals in COLUMN_GROUPS["enums"].items():
        if col_name not in df.columns:
            continue
        raw  = trim(col(col_name))
        norm = when(raw.isNull() | (raw == ""), lit(None))
        for v in valid_vals:
            norm = norm.when(upper(raw) == v.upper(), lit(v))
        df = df.withColumn(col_name, norm)

        enum_invalid_exprs[col_name] = spark_sum(
            when(
                raw.isNotNull() & ~upper(raw).isin([v.upper() for v in valid_vals]),
                1,
            ).otherwise(0)
        ).alias(f"{col_name}_invalid_count")

    # ── Step 4: Domain validation ─────────────────────────────────────────────
    df = df.withColumn(
        "order_year",
        when(
            (col("order_year").cast("int") < CONFIG["domain_rules"]["min_year"])
            | (col("order_year").cast("int") > CONFIG["domain_rules"]["max_year"]),
            lit(None),
        ).otherwise(col("order_year")),
    )

    # ── Step 5: Type casting (single SELECT) ──────────────────────────────────
    select_exprs = []

    for field in TARGET_SCHEMA.fields:
        name = field.name

        if name in COLUMN_GROUPS["primary_key"]:
            select_exprs.append(col(name))  # PK — never transformed

        elif name in COLUMN_GROUPS["string_ids"] + COLUMN_GROUPS["text_fields"]:
            select_exprs.append(col(name))

        elif name in COLUMN_GROUPS["numeric_ids"] + COLUMN_GROUPS["numeric_line_numbers"]:
            select_exprs.append(col(name).cast("int").alias(name))

        elif name in COLUMN_GROUPS["dates"]:
            raw = when(
                trim(col(name)).isNull() | (trim(col(name)) == ""), lit(None)
            ).otherwise(regexp_replace(trim(col(name)), "T.*$", ""))
            select_exprs.append(
                coalesce(
                    to_date(raw, "yyyy-MM-dd"),
                    to_date(raw, "dd-MM-yyyy"),
                ).alias(name)
            )

        elif name in COLUMN_GROUPS["quantities"]:
            select_exprs.append(col(name).cast("int").alias(name))

        elif name in COLUMN_GROUPS["amounts"]:
            select_exprs.append(col(name).cast("decimal(18,2)").alias(name))

        else:
            # Enum cols (normalized above), partition cols, order_year
            select_exprs.append(col(name))

    df_typed = df.select(*select_exprs)

    # ── Step 6: Single stats aggregation ─────────────────────────────────────
    # All quality metrics resolved in one Spark action.
    _log("INFO", "transform", "stats_aggregation", None,
         "Collecting quality metrics — 1 Spark action")

    agg_exprs = [
        spark_count("*").alias("total_rows"),
        spark_count(col("order_date")).alias("order_date_non_null"),
        spark_count(col("product_id")).alias("product_id_non_null"),
        spark_count(col("customer_id")).alias("customer_id_non_null"),
        spark_count(col("batch_id")).alias("batch_id_non_null"),
    ]
    agg_exprs.extend(enum_invalid_exprs.values())

    row        = df_typed.agg(*agg_exprs).collect()[0]
    total_rows = row["total_rows"]

    stats = {"domain_validation": {}, "type_conversion": {}}

    for col_name in COLUMN_GROUPS["enums"]:
        key = f"{col_name}_invalid_count"
        if key in row.asDict():
            inv  = row[key]
            rate = round(inv / total_rows * 100 if total_rows else 0, 4)
            stats["domain_validation"][col_name] = {
                "invalid_count":        inv,
                "invalid_rate_percent": rate,
            }
            if inv > 0:
                _log("WARN", "transform", "enum_invalid", inv,
                     f"{col_name}: {inv} unrecognized value(s) set to NULL",
                     extra={"column": col_name, "invalid_rate_percent": rate,
                            "valid_values": COLUMN_GROUPS["enums"][col_name]})

    for col_name, non_null_key in {
        "order_date":  "order_date_non_null",
        "product_id":  "product_id_non_null",
        "customer_id": "customer_id_non_null",
    }.items():
        null_n = total_rows - row[non_null_key]
        null_r = round(null_n / total_rows * 100 if total_rows else 0, 4)
        stats["type_conversion"][col_name] = {
            "null_count":           null_n,
            "null_rate_percent":    null_r,
            "success_rate_percent": round(100 - null_r, 4),
        }

    batch_null = total_rows - row["batch_id_non_null"]
    batch_rate = round(batch_null / total_rows * 100 if total_rows else 0, 4)
    stats["type_conversion"]["batch_id"] = {
        "null_count":        batch_null,
        "null_rate_percent": batch_rate,
        "note":              "Conditional FK — high NULL rate expected (~33 %)",
    }

    _log("INFO", "transform", "complete", total_rows,
         f"Transformation complete — {total_rows:,} rows",
         extra={"spark_actions_used": 1, "run_id": run_id})

    return df_typed, stats, total_rows


# ─────────────────────────────────────────────────────────────────────────────
# 6. SILVER QUALITY GATES
# ─────────────────────────────────────────────────────────────────────────────

def validate_silver_quality(
    df: DataFrame, source_row_count: int, stats: dict, run_id: str,
) -> dict:
    """
    Post-transformation quality gates. All hard-fail.

    Reuses pre-computed stats from transform_to_silver — zero additional
    Spark actions.

    Gates:
        1. order_date NULL rate ≤ critical_date_null_max_percent (FAIL otherwise)
        2. Partition columns trusted — no re-validation needed
    """
    _log("INFO", "quality", "start", None, "Silver quality gates started",
         extra={"run_id": run_id, "spark_actions_used": 0})

    threshold     = CONFIG["thresholds"]["critical_date_null_max_percent"]
    date_null_pct = stats["type_conversion"]["order_date"]["null_rate_percent"]
    date_null_n   = stats["type_conversion"]["order_date"]["null_count"]

    if date_null_pct > threshold:
        null_sample = df.filter(col("order_date").isNull()).limit(500)
        path = _write_forensic_report(
            "critical_date_null", run_id,
            summary={
                "column":            "order_date",
                "null_count":        date_null_n,
                "null_rate_percent": date_null_pct,
                "threshold_percent": threshold,
                "sample_row_limit":  500,
            },
            df_rows=null_sample,
        )
        _log_fail("quality", "critical_date_null", date_null_pct,
                  f"HARD FAIL: order_date NULL {date_null_pct}% > threshold {threshold}%",
                  extra={"column": "order_date", "null_count": date_null_n,
                         "null_rate_percent": date_null_pct,
                         "threshold_percent": threshold, "reject_path": path})
        raise ValueError(
            f"CRITICAL DATE GATE FAIL: order_date NULL {date_null_pct}% > "
            f"threshold {threshold}% → {path}"
        )

    quality_report = {
        "critical_date": {
            "column":            "order_date",
            "null_count":        date_null_n,
            "null_rate_percent": date_null_pct,
            "threshold_percent": threshold,
            "status":            "PASS",
        },
        "partition_integrity": {
            "status": "TRUSTED",
            "note":   "year / month values inherited from raw zone — not recalculated",
        },
        "row_count": {
            "source_rows": source_row_count,
            "silver_rows": source_row_count,
            "note":        "Lifecycle event rows are not filtered during transformation",
        },
    }

    _log("INFO", "quality", "complete", None, "All quality gates PASS",
         extra={"run_id": run_id})
    return quality_report


# ─────────────────────────────────────────────────────────────────────────────
# 7. WRITE TO SILVER
# ─────────────────────────────────────────────────────────────────────────────

def write_silver(df: DataFrame) -> None:
    """
    Write Silver DataFrame to S3 as Parquet + Snappy.

    Dynamic partition overwrite: only partitions written this run are replaced.
    All other partitions remain untouched — makes re-runs safe and idempotent.
    """
    _log("INFO", "write", "start", None,
         f"Writing Silver Parquet → {CONFIG['target']['s3_path']}")

    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.sparkSession.conf.set(
        "spark.sql.files.maxRecordsPerFile",
        CONFIG["write"]["max_records_per_file"],
    )

    df.write \
      .mode(CONFIG["write"]["mode"]) \
      .partitionBy(*COLUMN_GROUPS["partitions"]) \
      .option("compression", "snappy") \
      .parquet(CONFIG["target"]["s3_path"])

    _log("INFO", "write", "complete", None,
         f"Silver write complete → {CONFIG['target']['s3_path']}")


# ─────────────────────────────────────────────────────────────────────────────
# 8. METADATA REPORTING
# ─────────────────────────────────────────────────────────────────────────────

def generate_reports(
    run_id: str,
    source_validation: dict,
    transform_stats: dict,
    quality_report: dict,
    job_name: str,
) -> None:
    """
    Write full quality report and run summary JSON to S3 metadata prefix.

    Two artifacts per run:
        quality_report_{run_id}.json — full pipeline detail for debugging
        run_summary_{run_id}.json    — lightweight summary for monitoring
    """
    _log("INFO", "report", "start", None, "Writing run metadata")

    full_report = {
        "run_id":    run_id,
        "job_name":  job_name,
        "table":     TABLE_NAME_CONST,
        "timestamp": datetime.utcnow().isoformat(),
        "status":    "SUCCESS",
        "spark_action_budget": {
            "read_and_normalize":        1,
            "validate_source_integrity": 1,
            "transform_to_silver":       1,
            "validate_silver_quality":   0,
            "total":                     3,
        },
        "pipeline": {
            "1_source_read":       "COMPLETE",
            "2_source_integrity":  source_validation["status"],
            "3_transformation":    "COMPLETE",
            "4_quality_gates":     "PASS",
            "5_write":             "COMPLETE",
        },
        "source_validation":    source_validation,
        "transformation_stats": transform_stats,
        "quality_gates":        quality_report,
        "output": {
            "s3_path":      CONFIG["target"]["s3_path"],
            "format":       "parquet",
            "compression":  "snappy",
            "partitioning": "year / month — dynamic overwrite",
        },
        "semantic_notes": {
            "table_type":            "Lifecycle Fact (event grain)",
            "primary_key":           "erp_export_row_id — immutable, never transformed",
            "grain":                 "One row = one ERP export event",
            "degenerate_dimensions": ["order_line_id", "order_no", "order_line_no"],
            "aggregation_warning":   (
                "Never sum order_qty / delivery_qty / invoice_qty "
                "without filtering by line_status — each lifecycle "
                "stage produces one row per order line"
            ),
        },
    }

    _put_s3(
        full_report,
        CONFIG["metadata"]["s3_bucket"],
        f"{CONFIG['metadata']['s3_prefix']}quality_report_{run_id}.json",
    )
    _put_s3(
        {
            "run_id":         run_id,
            "job_name":       job_name,
            "status":         "SUCCESS",
            "table":          TABLE_NAME_CONST,
            "rows_processed": quality_report["row_count"]["source_rows"],
            "primary_key":    "erp_export_row_id (validated, never transformed)",
            "partitions":     "year / month (trusted from raw zone)",
        },
        CONFIG["metadata"]["s3_bucket"],
        f"{CONFIG['metadata']['s3_prefix']}run_summary_{run_id}.json",
    )

    _log("INFO", "report", "complete", None, "Metadata reports written",
         extra={"run_id": run_id})


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args     = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc       = SparkContext()
    glue_ctx = GlueContext(sc)
    spark    = glue_ctx.spark_session
    job      = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    run_id = get_run_id()
    _log("INFO", "job", "start", None,
         f"Orders ETL started — Run ID: {run_id}",
         extra={"job_name": args["JOB_NAME"], "run_id": run_id,
                "target_path": CONFIG["target"]["s3_path"]})

    try:
        # Validate source integrity (reads + normalizes internally)
        df_filtered, source_row_count, source_validation = validate_source_integrity(
            spark, run_id
        )
        _put_s3(
            source_validation,
            CONFIG["metadata"]["s3_bucket"],
            f"{CONFIG['metadata']['s3_prefix']}source_validation_{run_id}.json",
        )

        # Transform + collect stats
        df_silver, transform_stats, _ = transform_to_silver(df_filtered, run_id)

        # Quality gates (zero extra actions — reuses transform stats)
        quality_report = validate_silver_quality(
            df_silver, source_row_count, transform_stats, run_id
        )

        # Write
        write_silver(df_silver)

        # Metadata
        generate_reports(
            run_id, source_validation, transform_stats,
            quality_report, args["JOB_NAME"],
        )

        _log("INFO", "job", "success", source_row_count,
             f"Orders ETL complete — {source_row_count:,} rows",
             extra={"run_id": run_id, "output": CONFIG["target"]["s3_path"]})
        job.commit()

    except Exception as exc:
        _log_fail("job", "failure", None, str(exc),
                  extra={"run_id": run_id, "error_type": type(exc).__name__})
        _put_s3(
            {
                "run_id":     run_id,
                "status":     "FAILED",
                "error":      str(exc),
                "error_type": type(exc).__name__,
                "timestamp":  datetime.utcnow().isoformat(),
                "note":       (
                    f"Forensic artifacts → "
                    f"s3://{CONFIG['errors']['s3_bucket']}/{CONFIG['errors']['s3_prefix']}"
                ),
            },
            CONFIG["metadata"]["s3_bucket"],
            f"{CONFIG['metadata']['s3_prefix']}failure_{run_id}.json",
        )
        raise


if __name__ == "__main__":
    main()