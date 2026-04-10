"""
etl_multi_table.py — Multi-Table ETL
Pharma Supply Chain Data Platform

Transforms 7 dimension tables and 3 fact tables from raw CSV into
typed, validated Silver Parquet. The orders fact table is handled
separately in etl_orders.py due to its volume and schema complexity.

Tables:
    Dimensions (7): customers, manufacturers, customer_plant_mapping,
                    plant_warehouse, products_master, products_annual_demand,
                    product_manufacturer_bridge
    Facts      (3): product_batches, batch_allocator_central, inventory_movements

Processing order:
    Phase 2 — all 7 dimensions (sequential, abort on first failure)
    Phase 3 — all 3 facts     (only after all dimensions succeed)

Failure design:
    - Every check is a hard fail. No WARN-and-continue for data integrity.
    - Every hard fail writes a forensic artefact to S3 before raising.
    - Failure in any dimension aborts before facts are touched.
    - active_run.json marker blocks concurrent executions.
    - Failure report written to S3 on any unhandled exception.

Key decisions:
    - Source CSV read with header=true (avoids catalog header-leak problem).
    - Column names normalized to lowercase on read (source uses mixed case).
    - Boolean 0/1 columns: cast via INT→BOOLEAN; direct cast("boolean") returns NULL on "0"/"1".
    - Dynamic partition overwrite: only written partitions replaced — re-runs are safe.
"""

import sys
import json
import traceback
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit,
    count as spark_count, year as spark_year,
    to_date, regexp_replace, trim, upper, coalesce, length,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, DateType, DoubleType, DecimalType,
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
    },
    "target": {
        "database": "curated_zone",
        "s3_base":  "s3://pharma-curated-dev/",
    },
    "commit": {
        "s3_bucket": "pharma-curated-dev",
        "s3_prefix": "_commit/",
    },
    "metadata": {
        "s3_bucket": "pharma-curated-dev",
        "s3_prefix": "etl-metadata/job2-multi-table/",
    },
    "errors": {
        "s3_bucket": "pharma-curated-dev",
        "s3_prefix": "etl-errors/job2-multi-table/",
    },
    "logs": {
        "s3_bucket": "pharma-curated-dev",
        "s3_prefix": "etl-logs/job2-multi-table/",
    },
    "thresholds": {
        "critical_date_null_max_percent": 2.0,
        "partition_mismatch_max_percent": 1.0,
        "date_parse_failure_max_percent": 5.0,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 2. TABLE CONFIGURATIONS
# ─────────────────────────────────────────────────────────────────────────────

TABLE_CONFIGS = {
    "customers": {
        "type":              "dimension",
        "pk":                "customer_id",
        "pk_type":           "string",
        "partitioned":       False,
        "date_columns":      ["contract_start_date", "contract_end_date"],
        "bigint_to_boolean": [],
        "string_to_boolean": {},
    },
    "manufacturers": {
        "type":              "dimension",
        "pk":                "manufacturer_id",
        "pk_type":           "string",
        "partitioned":       False,
        "date_columns":      [],
        "bigint_to_boolean": [],
        "string_to_boolean": {},
    },
    "customer_plant_mapping": {
        "type":              "dimension",
        "pk":                "mapping_id",
        "pk_type":           "string",
        "partitioned":       False,
        "date_columns":      [],
        "bigint_to_boolean": [],
        "string_to_boolean": {},
        "boolean_columns":   ["active_flag"],
    },
    "plant_warehouse": {
        "type":              "dimension",
        "pk":                "plant_id",
        "pk_type":           "int",
        "partitioned":       False,
        "date_columns":      [],
        "bigint_to_boolean": [],
        "string_to_boolean": {"cold_storage_available": {"Yes": True, "No": False}},
    },
    "products_master": {
        "type":              "dimension",
        "pk":                "product_id",
        "pk_type":           "int",
        "partitioned":       False,
        "date_columns":      [],
        # 0/1 integer-encoded booleans — two-step cast required
        "bigint_to_boolean": ["isessentialdrug", "coldchainrequired"],
        "string_to_boolean": {},
        "boolean_columns":   ["is_discontinued"],
    },
    "products_annual_demand": {
        "type":              "dimension",
        "pk":                "product_id",
        "pk_type":           "int",
        "partitioned":       False,
        "date_columns":      [],
        "bigint_to_boolean": [],
        "string_to_boolean": {},
    },
    "product_manufacturer_bridge": {
        "type":              "dimension",
        # Composite PK: neither column alone uniquely identifies a row
        "pk":                None,
        "composite_pk":      ["manufacturer_id", "product_id"],
        "pk_type":           "composite",
        "partitioned":       False,
        "date_columns":      [],
        "bigint_to_boolean": [],
        "string_to_boolean": {},
    },
    "product_batches": {
        "type":              "fact",
        "pk":                "batch_id",
        "pk_type":           "string",
        "partitioned":       True,
        "partition_columns": ["year"],
        "date_columns":      ["mfg_date", "expiry_date"],
        "bigint_to_boolean": ["coldchainrequired"],
        "string_to_boolean": {},
    },
    "batch_allocator_central": {
        "type":              "fact",
        "pk":                "allocation_id",
        "pk_type":           "string",
        "partitioned":       True,
        "partition_columns": ["year"],
        "date_columns":      ["allocation_date", "mfg_date", "expiry_date"],
        "bigint_to_boolean": ["coldchainrequired"],
        "string_to_boolean": {},
    },
    "inventory_movements": {
        "type":              "fact",
        "pk":                "movement_id",
        "pk_type":           "string",
        "partitioned":       True,
        "partition_columns": ["year"],
        "date_columns":      ["movement_date", "mfg_date", "expiry_date"],
        "bigint_to_boolean": ["coldchainrequired"],
        "string_to_boolean": {},
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 3. TARGET SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

SCHEMAS = {
    "customers": StructType([
        StructField("customer_id",         StringType(),  False),
        StructField("customer_code",       StringType(),  True),
        StructField("customer_name_desc",  StringType(),  True),
        StructField("customer_type",       StringType(),  True),
        StructField("district",            StringType(),  True),
        StructField("state",               StringType(),  True),
        StructField("region",              StringType(),  True),
        StructField("ownership_type",      StringType(),  True),
        StructField("contract_start_date", DateType(),    True),
        StructField("contract_end_date",   DateType(),    True),  # NULL for active contracts
    ]),
    "manufacturers": StructType([
        StructField("manufacturer_id",   StringType(), False),
        StructField("manufacturer_name", StringType(), True),
    ]),
    "customer_plant_mapping": StructType([
        StructField("mapping_id",    StringType(),  False),
        StructField("customer_id",   StringType(),  True),
        StructField("plant_code",    StringType(),  True),
        StructField("priority_flag", StringType(),  True),   # PRIMARY / SECONDARY / BACKUP
        StructField("active_flag",   BooleanType(), True),
    ]),
    "plant_warehouse": StructType([
        StructField("plant_id",               IntegerType(), False),
        StructField("plant_code",             StringType(),  True),
        StructField("plant_name",             StringType(),  True),
        StructField("plant_type",             StringType(),  True),  # Central_DC / Regional_DC
        StructField("state",                  StringType(),  True),
        StructField("district",               StringType(),  True),
        StructField("region",                 StringType(),  True),
        StructField("latitude",               DoubleType(),  True),
        StructField("longitude",              DoubleType(),  True),
        StructField("total_storage_capacity", IntegerType(), True),
        StructField("cold_storage_available", BooleanType(), True),
        StructField("cold_storage_capacity",  IntegerType(), True),
        StructField("status",                 StringType(),  True),
        StructField("parent_plant_code",      StringType(),  True),  # NULL for Central DCs
    ]),
    "products_master": StructType([
        StructField("product_id",           IntegerType(),      False),
        StructField("productcode",          StringType(),       True),  # Family key — NOT unique
        StructField("hsncode",              IntegerType(),      True),
        StructField("generic_composition",  StringType(),       True),
        StructField("strength_value",       StringType(),       True),  # Mixed format — kept STRING
        StructField("strength_unit",        StringType(),       True),
        StructField("formulation",          StringType(),       True),
        StructField("packing_type",         StringType(),       True),
        StructField("pack_size_label",      StringType(),       True),
        StructField("therapeutic_segment",  StringType(),       True),
        StructField("isessentialdrug",      BooleanType(),      True),
        StructField("coldchainrequired",    BooleanType(),      True),
        StructField("gst_rate",             DecimalType(5, 2),  True),
        StructField("product_name",         StringType(),       True),
        StructField("formulation_code",     StringType(),       True),
        StructField("pack_count",           IntegerType(),      True),  # 9 known bad values (P1-002)
        StructField("generic_code",         StringType(),       True),
        StructField("is_discontinued",      BooleanType(),      True),
        StructField("price",                DecimalType(18, 2), True),
        StructField("short_composition1",   StringType(),       True),
        StructField("short_composition2",   StringType(),       True),
        StructField("shelflife_months",     IntegerType(),      True),
    ]),
    "products_annual_demand": StructType([
        StructField("product_id",          IntegerType(),      False),
        StructField("product_code",        StringType(),       True),
        StructField("generic",             StringType(),       True),
        StructField("formulation",         StringType(),       True),
        StructField("shelf_life_months",   IntegerType(),      True),
        StructField("pack_units_per_pack", IntegerType(),      True),
        StructField("unit_mrp",            DecimalType(18, 2), True),
        StructField("demand_tier",         StringType(),       True),
        StructField("annual_target_units", IntegerType(),      True),
        StructField("batches_per_year",    IntegerType(),      True),
        StructField("total_batches",       IntegerType(),      True),
        StructField("nominal_batch_size",  IntegerType(),      True),
    ]),
    "product_manufacturer_bridge": StructType([
        StructField("manufacturer_id",     StringType(),       True),
        StructField("manufacturer_name",   StringType(),       True),
        StructField("production_level",    StringType(),       True),
        StructField("product_id",          IntegerType(),      True),
        StructField("generic_composition", StringType(),       True),
        StructField("therapeutic_segment", StringType(),       True),
        StructField("formulation",         StringType(),       True),
        StructField("strength_value",      StringType(),       True),
        StructField("strength_unit",       StringType(),       True),
        StructField("price",               DecimalType(18, 2), True),
    ]),
    "product_batches": StructType([
        StructField("batch_id",            StringType(),       False),
        StructField("batch_number",        StringType(),       True),
        StructField("lot_number",          StringType(),       True),
        StructField("product_id",          IntegerType(),      True),
        StructField("product_code",        StringType(),       True),
        StructField("manufacturer_id",     StringType(),       True),
        StructField("manufacturer_name",   StringType(),       True),
        StructField("mfg_date",            DateType(),         True),
        StructField("expiry_date",         DateType(),         True),
        StructField("mfg_year",            IntegerType(),      True),
        StructField("formulation",         StringType(),       True),
        StructField("generic_name",        StringType(),       True),
        StructField("pack_units_per_pack", IntegerType(),      True),
        StructField("batch_size_units",    IntegerType(),      True),
        StructField("unit_mrp",            DecimalType(18, 2), True),
        StructField("coldchainrequired",   BooleanType(),      True),
        StructField("year",                StringType(),       False),  # partition key
    ]),
    "batch_allocator_central": StructType([
        StructField("allocation_id",    StringType(),       False),
        StructField("batch_id",         StringType(),       True),
        StructField("batch_number",     StringType(),       True),
        StructField("lot_number",       StringType(),       True),
        StructField("product_id",       IntegerType(),      True),
        StructField("manufacturer_id",  StringType(),       True),
        StructField("plant_id",         IntegerType(),      True),
        StructField("operational_zone", StringType(),       True),
        StructField("units_allocated",  IntegerType(),      True),
        StructField("allocation_date",  DateType(),         True),
        StructField("mfg_date",         DateType(),         True),
        StructField("expiry_date",      DateType(),         True),
        StructField("allocation_year",  IntegerType(),      True),
        StructField("demand_tier",      StringType(),       True),
        StructField("formulation",      StringType(),       True),
        StructField("coldchainrequired", BooleanType(),     True),
        StructField("batch_size_units", IntegerType(),      True),
        StructField("year",             StringType(),       False),  # partition key
    ]),
    "inventory_movements": StructType([
        StructField("movement_id",       StringType(),  False),
        StructField("movement_type",     StringType(),  True),   # IN / TRANSFER / Central_Movement
        StructField("batch_id",          StringType(),  True),
        StructField("batch_number",      StringType(),  True),
        StructField("lot_number",        StringType(),  True),
        StructField("allocation_id",     StringType(),  True),   # NULL for ~70 % — optional FK
        StructField("product_id",        IntegerType(), True),
        StructField("manufacturer_id",   StringType(),  True),
        StructField("from_plant_id",     IntegerType(), True),   # NULL for IN movements
        StructField("to_plant_id",       IntegerType(), True),
        StructField("units",             IntegerType(), True),
        StructField("movement_date",     DateType(),    True),
        StructField("movement_year",     IntegerType(), True),
        StructField("mfg_date",          DateType(),    True),
        StructField("expiry_date",       DateType(),    True),
        StructField("demand_tier",       StringType(),  True),
        StructField("formulation",       StringType(),  True),
        StructField("coldchainrequired", BooleanType(), True),
        StructField("operational_zone",  StringType(),  True),
        StructField("year",              StringType(),  False),  # partition key
    ]),
}


# ─────────────────────────────────────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────────────────────────────────────

class JobLogger:
    """
    Structured JSON logger.

    Each entry goes to stdout immediately (CloudWatch Logs ingestion).
    All entries are accumulated and flushed to S3 at job end for post-run
    analysis. The S3 log is written even on failure.
    """

    def __init__(self, run_id: str, job_name: str) -> None:
        self.run_id      = run_id
        self.job_name    = job_name
        self.entries     = []
        self.error_count = 0
        self.warn_count  = 0

    def log(self, level: str, phase: str, metric: str, value,
            message: str, table: str = None, extra: dict = None) -> None:
        """Emit one structured log entry."""
        entry = {
            "timestamp":  datetime.utcnow().isoformat() + "Z",
            "run_id":     self.run_id,
            "level":      level,
            "job_name":   self.job_name,
            "table_name": table,
            "phase":      phase,
            "metric":     metric,
            "value":      value,
            "message":    message,
        }
        if extra:
            entry["context"] = extra
        if level == "ERROR":
            self.error_count += 1
        elif level == "WARN":
            self.warn_count  += 1
        self.entries.append(entry)
        print(json.dumps(entry))

    def fail(self, phase: str, metric: str, value, message: str,
             table: str = None, extra: dict = None) -> None:
        """Convenience — always ERROR level. Call immediately before raising."""
        self.log("ERROR", phase, metric, value, message, table, extra)

    def flush_to_s3(self) -> str:
        """Write accumulated log to S3.  Returns the S3 URI."""
        key = f"{CONFIG['logs']['s3_prefix']}run_{self.run_id}.json"
        boto3.client("s3").put_object(
            Bucket=CONFIG["logs"]["s3_bucket"],
            Key=key,
            Body=json.dumps({
                "run_id":        self.run_id,
                "job_name":      self.job_name,
                "total_entries": len(self.entries),
                "error_count":   self.error_count,
                "warn_count":    self.warn_count,
                "entries":       self.entries,
            }, indent=2, default=str),
        )
        return f"s3://{CONFIG['logs']['s3_bucket']}/{key}"


# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def get_run_id() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")


def _put_s3(content, bucket: str, key: str) -> None:
    if isinstance(content, (dict, list)):
        content = json.dumps(content, indent=2, default=str)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content)


def _write_forensic_report(
    table_name: str,
    error_type: str,
    run_id: str,
    summary: dict,
    logger: JobLogger,
    df_rows: DataFrame = None,
    write_csv: bool = True,
) -> str:
    """
    Write a forensic error report to S3 before any hard fail.

    Produces:
        report.json     — structured summary with row counts and context
        parquet/        — offending rows in columnar format (when df_rows supplied)
        csv/            — offending rows in human-readable format (when write_csv=True)

    Returns the S3 reject prefix so it can be included in the exception message.
    """
    prefix = (
        f"s3://{CONFIG['errors']['s3_bucket']}/"
        f"{CONFIG['errors']['s3_prefix']}"
        f"{table_name}/{error_type}/run_id={run_id}/"
    )
    report = {
        "run_id":      run_id,
        "table_name":  table_name,
        "error_type":  error_type,
        "timestamp":   datetime.utcnow().isoformat(),
        "reject_path": prefix,
        "summary":     summary,
    }
    _put_s3(
        report,
        CONFIG["errors"]["s3_bucket"],
        f"{CONFIG['errors']['s3_prefix']}{table_name}/{error_type}/run_id={run_id}/report.json",
    )

    if df_rows is not None:
        df_rows.write.mode("overwrite").option("compression", "snappy") \
               .parquet(f"{prefix}parquet/")
        if write_csv:
            df_rows.coalesce(1).write.mode("overwrite") \
                   .option("header", True).csv(f"{prefix}csv/")
        logger.log("INFO", "forensic_write", "reject_path", prefix,
                   f"Reject rows written", table_name,
                   extra={"error_type": error_type, "formats": ["parquet", "csv"]})

    return prefix


# ─────────────────────────────────────────────────────────────────────────────
# CONCURRENT RUN GUARD
# ─────────────────────────────────────────────────────────────────────────────

def assert_no_active_run(logger: JobLogger) -> None:
    """
    Abort if another run is IN_PROGRESS.

    The active_run.json marker is written at start and updated to COMMITTED or
    FAILED at end.  Reading IN_PROGRESS means a concurrent run is active.
    Starting now would cause two jobs writing to the same curated partitions.
    """
    logger.log("INFO", "preflight", "concurrent_run_check", None,
               "Checking for concurrent runs")
    try:
        obj    = boto3.client("s3").get_object(
            Bucket=CONFIG["commit"]["s3_bucket"],
            Key=f"{CONFIG['commit']['s3_prefix']}active_run.json",
        )
        active = json.loads(obj["Body"].read())

        if active.get("status") == "IN_PROGRESS":
            msg = (
                f"CONCURRENT RUN BLOCKED — "
                f"active run_id={active.get('run_id')} "
                f"started={active.get('timestamp')}"
            )
            logger.fail("preflight", "concurrent_run", None, msg,
                        extra={"active_run_id": active.get("run_id"),
                               "active_since":  active.get("timestamp")})
            raise RuntimeError(msg)

        logger.log("INFO", "preflight", "previous_run_status", active.get("status"),
                   f"Previous run: {active.get('run_id')} — {active.get('status')}")

    except ClientError:
        logger.log("INFO", "preflight", "no_previous_run", None,
                   "No previous run marker found — clean start")


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE READ + COLUMN NORMALIzATION
# ─────────────────────────────────────────────────────────────────────────────

def read_and_normalize(
    spark, table_name: str, logger: JobLogger,
) -> tuple:
    """
    Read raw CSV from S3 with header=true and normalize column names.

    Reading directly from S3 (via the catalog S3 location) with header=true
    lets Spark handle the header row — no post-read filter needed. This avoids
    the catalog header-leak problem handled in etl_orders.py.

    Column normalization: strip whitespace, lowercase, spaces → underscore.
    Eliminates the mismatch between source naming conventions and Glue Catalog.

    Returns:
        tuple: (df_normalized, row_count)
    """
    logger.log("INFO", "read_source", "start", None,
               "Reading and normalizing source", table_name)

    meta       = boto3.client("glue").get_table(
        DatabaseName=CONFIG["source"]["database"], Name=table_name
    )
    s3_loc     = meta["Table"]["StorageDescriptor"]["Location"]

    df_raw     = (
        spark.read
             .option("header",                   "true")
             .option("inferSchema",              "false")
             .option("multiLine",                "false")
             .option("ignoreLeadingWhiteSpace",  "true")
             .option("ignoreTrailingWhiteSpace", "true")
             .csv(s3_loc)
    )

    # Action: count
    row_count    = df_raw.count()
    original     = df_raw.columns
    normalized   = [c.strip().lower().replace(" ", "_") for c in original]
    df_norm      = df_raw.toDF(*normalized)

    logger.log("INFO", "read_source", "row_count", row_count,
               f"{row_count:,} rows read", table_name,
               extra={"s3_location": s3_loc,
                      "columns_normalized": len(normalized),
                      "sample_renames": [
                          f"{original[i]} → {normalized[i]}"
                          for i in range(min(3, len(original)))
                      ]})
    return df_norm, row_count


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_source(
    spark, table_name: str, cfg: dict,
    logger: JobLogger, run_id: str,
) -> tuple:
    """
    Hard-fail source validation for one table.

    Spark actions: 1 (read count) + PK checks (1 or 2 additional counts)

    Checks (all hard-fail):
        1. Row count > 0
        2. PK (or composite PK): zero NULLs, zero duplicates
           Forensic artefact written before every raise.

    Returns:
        tuple: (df_normalized, row_count)
    """
    logger.log("INFO", "source_validation", "start", None,
               "Source validation started", table_name)

    df, total = read_and_normalize(spark, table_name, logger)

    if cfg["type"] == "dimension":
        df = df.cache()

    if total == 0:
        logger.fail("source_validation", "empty_table", 0,
                    "HARD FAIL: table has 0 rows", table_name)
        raise ValueError(f"{table_name}: source table has 0 rows")

    # ── Composite PK ─────────────────────────────────────────────────────────
    if cfg["pk_type"] == "composite":
        pks = cfg["composite_pk"]

        null_n = df.filter(col(pks[0]).isNull() | col(pks[1]).isNull()).count()
        if null_n > 0:
            null_df = df.filter(col(pks[0]).isNull() | col(pks[1]).isNull()).limit(500)
            path = _write_forensic_report(
                table_name, "pk_null_violation", run_id,
                summary={"null_count": null_n, "pk_columns": pks,
                         "total_rows": total},
                logger=logger, df_rows=null_df,
            )
            logger.fail("source_validation", "pk_null", null_n,
                        f"HARD FAIL: composite PK has {null_n} NULL rows",
                        table_name,
                        extra={"pk_columns": pks, "reject_path": path})
            raise ValueError(f"{table_name}: composite PK NULL ({null_n} rows) → {path}")

        dup_df  = (df.groupBy(*pks)
                     .agg(spark_count("*").alias("cnt"))
                     .filter(col("cnt") > 1))
        dup_n   = dup_df.count()
        if dup_n > 0:
            path = _write_forensic_report(
                table_name, "pk_duplicate_violation", run_id,
                summary={"duplicate_count": dup_n, "pk_columns": pks,
                         "total_rows": total},
                logger=logger, df_rows=dup_df,
            )
            logger.fail("source_validation", "pk_duplicate", dup_n,
                        f"HARD FAIL: composite PK has {dup_n} duplicates",
                        table_name,
                        extra={"pk_columns": pks, "reject_path": path})
            raise ValueError(f"{table_name}: composite PK duplicates ({dup_n}) → {path}")

    # ── Single-column PK ─────────────────────────────────────────────────────
    else:
        pk = cfg["pk"]

        null_n = df.filter(col(pk).isNull()).count()
        if null_n > 0:
            null_df = df.filter(col(pk).isNull()).limit(500)
            path = _write_forensic_report(
                table_name, "pk_null_violation", run_id,
                summary={"null_count": null_n, "pk_column": pk,
                         "null_rate_percent": round(null_n / total * 100, 4),
                         "total_rows": total},
                logger=logger, df_rows=null_df,
            )
            logger.fail("source_validation", "pk_null", null_n,
                        f"HARD FAIL: PK '{pk}' has {null_n} NULL values",
                        table_name,
                        extra={"pk_column": pk,
                               "null_rate_percent": round(null_n / total * 100, 4),
                               "total_rows": total,
                               "reject_path": path})
            raise ValueError(f"{table_name}: PK null violation ({null_n} rows) → {path}")

        dup_df  = (df.groupBy(pk)
                     .agg(spark_count("*").alias("occurrence_count"))
                     .filter(col("occurrence_count") > 1))
        dup_n   = dup_df.count()
        if dup_n > 0:
            # Occurrence distribution: how many keys appear 2× vs 3× etc.
            dist = (dup_df.groupBy("occurrence_count")
                          .agg(spark_count("*").alias("key_count"))
                          .orderBy("occurrence_count")
                          .collect())
            full_dup = df.join(dup_df.select(pk), on=pk, how="inner")
            path = _write_forensic_report(
                table_name, "pk_duplicate_violation", run_id,
                summary={"unique_duplicate_keys":   dup_n,
                         "pk_column":               pk,
                         "occurrence_distribution": [r.asDict() for r in dist],
                         "total_rows":              total},
                logger=logger, df_rows=full_dup,
            )
            logger.fail("source_validation", "pk_duplicate", dup_n,
                        f"HARD FAIL: PK '{pk}' has {dup_n} duplicate keys",
                        table_name,
                        extra={"pk_column":               pk,
                               "unique_duplicate_keys":   dup_n,
                               "occurrence_distribution": [r.asDict() for r in dist],
                               "reject_path":             path})
            raise ValueError(f"{table_name}: PK duplicate violation ({dup_n} keys) → {path}")

    logger.log("INFO", "source_validation", "pass", total,
               f"Source validation PASS — {total:,} unique rows", table_name)
    return df, total


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMATION
# ─────────────────────────────────────────────────────────────────────────────

def transform_table(
    df_raw: DataFrame, table_name: str, cfg: dict,
    schema: StructType, source_rows: int,
    logger: JobLogger, run_id: str,
) -> DataFrame:
    """
    Apply all transformation steps.

    Steps (no Spark actions until date parse validation):
        1. String cleaning    — PK excluded
        2. Date parsing       — three-format coalesce; failure rate checked
        3. Boolean conversion — Yes/No strings and 0/1 integers
        4. Type casting       — single SELECT routes all columns

    Date parse failure check uses additional actions (one count per date column
    with parse failures > 0). This is intentional — date failures are hard-fail
    conditions requiring forensic output.
    """
    logger.log("INFO", "transform", "start", None, "Transformation started", table_name,
               extra={"source_rows": source_rows})

    df = df_raw

    pk_cols = (
        set(cfg["composite_pk"]) if cfg["pk_type"] == "composite"
        else ({cfg["pk"]} if cfg["pk"] else set())
    )

    # ── Step 1: String cleaning (PK excluded) ────────────────────────────────
    for field in schema.fields:
        if field.name in pk_cols:
            continue
        if isinstance(field.dataType, StringType) \
                and field.name not in cfg.get("date_columns", []):
            df = df.withColumn(
                field.name,
                when(
                    trim(col(field.name)).isNull() | (trim(col(field.name)) == ""),
                    lit(None),
                ).otherwise(trim(col(field.name))),
            )

    # ── Step 2: Date parsing with three-format coalesce ──────────────────────
    date_parse_map = {}
    for col_name in cfg.get("date_columns", []):
        if col_name not in df.columns:
            continue
        cleaned = when(
            trim(col(col_name)).isNull()
            | (trim(col(col_name)) == "")
            | (length(trim(col(col_name))) < 8),
            lit(None),
        ).otherwise(regexp_replace(trim(col(col_name)), "T.*$", ""))
        df = df.withColumn(f"{col_name}_clean", cleaned)

        parsed = coalesce(
            to_date(col(f"{col_name}_clean"), "yyyy-MM-dd"),
            to_date(col(f"{col_name}_clean"), "dd-MM-yyyy"),
            to_date(col(f"{col_name}_clean"), "yyyy/MM/dd"),
        )
        df = df.withColumn(f"{col_name}_parsed", parsed)
        date_parse_map[col_name] = {
            "clean":  f"{col_name}_clean",
            "parsed": f"{col_name}_parsed",
        }

    # ── Step 3: Boolean conversions ───────────────────────────────────────────
    for col_name in cfg.get("string_to_boolean", {}):
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(upper(trim(col(col_name))) == "YES", lit(True))
                .when(upper(trim(col(col_name))) == "NO",  lit(False))
                .otherwise(lit(None)),
            )

    # Two-step cast for 0/1 integer-encoded booleans.
    # direct cast("boolean") on "0"/"1" string returns NULL — incorrect.
    for col_name in cfg.get("bigint_to_boolean", []):
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(col(col_name) == 1, lit(True))
                .when(col(col_name) == 0, lit(False))
                .otherwise(lit(None)),
            )

    # ── Step 4: Type casting (single SELECT) ─────────────────────────────────
    select_exprs = []
    for field in schema.fields:
        name = field.name

        if name in date_parse_map:
            select_exprs.append(col(date_parse_map[name]["parsed"]).alias(name))

        elif isinstance(field.dataType, IntegerType):
            select_exprs.append(col(name).cast("int").alias(name))

        elif isinstance(field.dataType, DoubleType):
            select_exprs.append(col(name).cast("double").alias(name))

        elif isinstance(field.dataType, DecimalType):
            select_exprs.append(col(name).cast(field.dataType).alias(name))

        elif isinstance(field.dataType, BooleanType):
            # Handle any remaining string-encoded booleans not converted above
            select_exprs.append(
                when(upper(trim(col(name))) == "TRUE",  lit(True))
                .when(upper(trim(col(name))) == "FALSE", lit(False))
                .otherwise(lit(None))
                .alias(name)
            )
        else:
            select_exprs.append(col(name))

    df_typed = df.select(*select_exprs)

    # ── Date parse failure validation (hard-fail above threshold) ────────────
    for col_name, cols in date_parse_map.items():
        fail_n = df.filter(
            col(cols["clean"]).isNotNull() & col(cols["parsed"]).isNull()
        ).count()

        if fail_n == 0:
            continue

        rate      = round(fail_n / source_rows * 100 if source_rows else 0, 4)
        threshold = CONFIG["thresholds"]["date_parse_failure_max_percent"]

        if rate > threshold:
            sample = df.filter(
                col(cols["clean"]).isNotNull() & col(cols["parsed"]).isNull()
            ).select(*list(pk_cols), cols["clean"]).limit(500)

            path = _write_forensic_report(
                table_name, f"date_parse_failure_{col_name}", run_id,
                summary={"column":           col_name,
                         "failure_count":    fail_n,
                         "failure_rate":     rate,
                         "threshold":        threshold,
                         "sample_limit":     500},
                logger=logger, df_rows=sample,
            )
            logger.fail("transform", "date_parse_threshold", rate,
                        f"HARD FAIL: {col_name} parse failure {rate}% > threshold {threshold}%",
                        table_name,
                        extra={"column":        col_name,
                               "failure_count": fail_n,
                               "failure_rate":  rate,
                               "threshold":     threshold,
                               "reject_path":   path})
            raise ValueError(
                f"{table_name}: {col_name} date parse failure {rate}% "
                f"exceeds threshold {threshold}% → {path}"
            )

        logger.log("WARN", "transform", "date_parse_failures", fail_n,
                   f"{col_name}: {fail_n} parse failures ({rate}%) — below threshold, values set NULL",
                   table_name,
                   extra={"column": col_name, "failure_rate": rate,
                          "threshold": threshold})

    if cfg["type"] == "dimension":
        df_raw.unpersist()

    logger.log("INFO", "transform", "complete", source_rows,
               "Transformation complete", table_name)
    return df_typed


# ─────────────────────────────────────────────────────────────────────────────
# QUALITY GATES
# ─────────────────────────────────────────────────────────────────────────────

def validate_quality(
    df: DataFrame, table_name: str, source_rows: int,
    cfg: dict, logger: JobLogger, run_id: str,
) -> dict:
    """
    Post-transformation quality gates. All hard-fail.

    Gates:
        1. Row count — no rows may be lost (FAIL)
        2. Critical date NULL rate — first date column ≤ threshold (FAIL)
        3. Partition year integrity — year column must match date year (FAIL)
    """
    logger.log("INFO", "quality", "start", None, "Quality gates started", table_name)

    quality = {}

    # ── Gate 1: Row preservation ──────────────────────────────────────────────
    silver_n = df.count()
    if silver_n < source_rows:
        lost = source_rows - silver_n
        path = _write_forensic_report(
            table_name, "row_loss", run_id,
            summary={"source_rows": source_rows, "silver_rows": silver_n,
                     "rows_lost": lost,
                     "loss_rate_percent": round(lost / source_rows * 100, 4)},
            logger=logger,
        )
        logger.fail("quality", "row_loss", silver_n,
                    f"HARD FAIL: {lost} rows lost during transformation",
                    table_name,
                    extra={"source_rows": source_rows, "silver_rows": silver_n,
                           "rows_lost":   lost, "reject_path": path})
        raise ValueError(
            f"{table_name}: row loss — {source_rows:,} source → {silver_n:,} silver "
            f"({lost:,} lost) → {path}"
        )

    quality["row_count"] = {
        "source": source_rows, "silver": silver_n, "preservation_rate": "100%"
    }

    # ── Gate 2: Critical date NULL rate (facts only) ──────────────────────────
    if cfg["type"] == "fact" and cfg.get("date_columns"):
        crit_date = cfg["date_columns"][0]
        null_n    = df.filter(col(crit_date).isNull()).count()
        null_rate = round(null_n / silver_n * 100 if silver_n else 0, 4)
        threshold = CONFIG["thresholds"]["critical_date_null_max_percent"]

        if null_rate > threshold:
            sample = df.filter(col(crit_date).isNull()).limit(500)
            path = _write_forensic_report(
                table_name, f"critical_date_null_{crit_date}", run_id,
                summary={"column": crit_date, "null_count": null_n,
                         "null_rate": null_rate, "threshold": threshold,
                         "sample_limit": 500},
                logger=logger, df_rows=sample,
            )
            logger.fail("quality", "critical_date_null", null_rate,
                        f"HARD FAIL: {crit_date} NULL {null_rate}% > threshold {threshold}%",
                        table_name,
                        extra={"column": crit_date, "null_count": null_n,
                               "null_rate": null_rate, "threshold": threshold,
                               "reject_path": path})
            raise ValueError(
                f"{table_name}: {crit_date} NULL rate {null_rate}% "
                f"exceeds threshold {threshold}% → {path}"
            )

        quality["critical_date"] = {
            "column": crit_date, "null_rate": null_rate,
            "threshold": threshold, "status": "PASS",
        }

    # ── Gate 3: Partition year integrity (partitioned facts) ──────────────────
    if cfg.get("partitioned"):
        date_col  = cfg["date_columns"][0]
        df_chk    = df.withColumn("_calc_year", spark_year(col(date_col)).cast("string"))
        mismatch  = df_chk.filter(
            col(date_col).isNotNull() & (col("year") != col("_calc_year"))
        ).count()
        mis_rate  = round(mismatch / silver_n * 100 if silver_n else 0, 4)
        threshold = CONFIG["thresholds"]["partition_mismatch_max_percent"]

        if mis_rate > threshold:
            sample = df_chk.filter(
                col(date_col).isNotNull() & (col("year") != col("_calc_year"))
            ).select(col(cfg["pk"]), col(date_col),
                     col("year"), col("_calc_year")).limit(500)
            path = _write_forensic_report(
                table_name, "partition_mismatch", run_id,
                summary={"mismatch_count": mismatch, "mismatch_rate": mis_rate,
                         "threshold": threshold, "date_column": date_col,
                         "sample_limit": 500},
                logger=logger, df_rows=sample,
            )
            logger.fail("quality", "partition_mismatch", mis_rate,
                        f"HARD FAIL: partition year mismatch {mis_rate}% > threshold {threshold}%",
                        table_name,
                        extra={"mismatch_count": mismatch, "mismatch_rate": mis_rate,
                               "threshold": threshold, "reject_path": path})
            raise ValueError(
                f"{table_name}: partition mismatch {mis_rate}% "
                f"exceeds threshold {threshold}% → {path}"
            )

        quality["partition_integrity"] = {
            "mismatch_rate": mis_rate, "threshold": threshold, "status": "PASS"
        }

    logger.log("INFO", "quality", "pass", None, "Quality gates PASS", table_name)
    return quality


# ─────────────────────────────────────────────────────────────────────────────
# WRITE TO CURATED
# ─────────────────────────────────────────────────────────────────────────────

def write_curated(
    df: DataFrame, table_name: str, cfg: dict, logger: JobLogger,
) -> str:
    """
    Write Silver DataFrame to curated S3 zone as Parquet
    Dynamic partition overwrite — only written partitions are replaced.
    """
    path = f"{CONFIG['target']['s3_base']}{cfg['type']}s/{table_name}/"
    logger.log("INFO", "write", "start", None, f"Writing → {path}", table_name)

    writer = df.write.mode("overwrite").option("compression", "snappy")
    if cfg.get("partitioned"):
        writer = writer.partitionBy(*cfg["partition_columns"])
    writer.parquet(path)

    logger.log("INFO", "write", "complete", None, f"Write complete → {path}", table_name)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# ATOMIC COMMIT PROTOCOL
# ─────────────────────────────────────────────────────────────────────────────

def _write_run_marker(run_id: str, status: str,
                      tables: list = None, logger: JobLogger = None) -> None:
    marker = {
        "run_id":           run_id,
        "status":           status,
        "timestamp":        datetime.utcnow().isoformat(),
        "tables_to_process": list(TABLE_CONFIGS.keys()) if status == "IN_PROGRESS" else None,
        "tables_processed": tables if status in ("COMMITTED", "FAILED") else None,
    }
    _put_s3(
        marker,
        CONFIG["commit"]["s3_bucket"],
        f"{CONFIG['commit']['s3_prefix']}active_run.json",
    )
    if logger:
        logger.log("INFO", "commit", "marker_written", None,
                   f"Run marker → {status}")


def _write_commit_marker(run_id: str, reports: dict, logger: JobLogger) -> None:
    _put_s3(
        {
            "run_id":           run_id,
            "status":           "COMMITTED",
            "committed_at":     datetime.utcnow().isoformat(),
            "tables_committed": list(reports.keys()),
            "row_counts": {
                t: r.get("source_rows", 0) for t, r in reports.items()
            },
        },
        CONFIG["commit"]["s3_bucket"],
        f"{CONFIG['commit']['s3_prefix']}SUCCESS",
    )
    logger.log("INFO", "commit", "success_marker", len(reports),
               f"SUCCESS — {len(reports)} tables committed")


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE-TABLE PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def process_table(
    spark, table_name: str, logger: JobLogger, run_id: str,
) -> dict:
    """Run the full pipeline for one table: validate → transform → quality → write."""
    cfg    = TABLE_CONFIGS[table_name]
    report = {
        "table_name":   table_name,
        "table_type":   cfg["type"],
        "started_at":   datetime.utcnow().isoformat(),
    }

    try:
        df_raw, source_rows = validate_source(spark, table_name, cfg, logger, run_id)
        df_silver           = transform_table(df_raw, table_name, cfg,
                                              SCHEMAS[table_name], source_rows,
                                              logger, run_id)
        quality             = validate_quality(df_silver, table_name, source_rows,
                                               cfg, logger, run_id)
        curated_path        = write_curated(df_silver, table_name, cfg, logger)

        report.update({
            "status":       "SUCCESS",
            "source_rows":  source_rows,
            "quality":      quality,
            "output_path":  curated_path,
            "completed_at": datetime.utcnow().isoformat(),
        })

    except Exception as exc:
        report.update({
            "status":       "FAILED",
            "error":        str(exc),
            "error_type":   type(exc).__name__,
            "traceback":    traceback.format_exc(),
            "completed_at": datetime.utcnow().isoformat(),
        })
        logger.fail("table_pipeline", "table_failed", None, str(exc), table_name,
                    extra={"traceback": traceback.format_exc()})
        raise

    return report


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

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    run_id = get_run_id()
    logger = JobLogger(run_id, args["JOB_NAME"])
    logger.log("INFO", "bootstrap", "start", None,
               f"Multi-Table ETL started — Run ID: {run_id}",
               extra={"job_name": args["JOB_NAME"]})

    try:
        assert_no_active_run(logger)
    except Exception:
        logger.flush_to_s3()
        raise

    _write_run_marker(run_id, "IN_PROGRESS", logger=logger)

    dimension_tables = [
        "customers", "manufacturers", "customer_plant_mapping",
        "plant_warehouse", "products_master", "products_annual_demand",
        "product_manufacturer_bridge",
    ]
    fact_tables = [
        "product_batches", "batch_allocator_central", "inventory_movements",
    ]

    all_reports: dict = {}

    try:
        # ── Phase 2: Dimensions (fail fast — no partial state) ────────────────
        logger.log("INFO", "phase2", "start", len(dimension_tables),
                   f"Processing {len(dimension_tables)} dimension tables")

        for table in dimension_tables:
            report = process_table(spark, table, logger, run_id)
            all_reports[table] = report
            logger.log("INFO", "phase2", "table_complete", None, f"✓ {table}", table)

        logger.log("INFO", "phase2", "complete", len(dimension_tables),
                   "All dimensions complete")

        # ── Phase 3: Facts (only after all dimensions pass) ───────────────────
        logger.log("INFO", "phase3", "start", len(fact_tables),
                   f"Processing {len(fact_tables)} fact tables")

        for table in fact_tables:
            report = process_table(spark, table, logger, run_id)
            all_reports[table] = report
            logger.log("INFO", "phase3", "table_complete", None, f"✓ {table}", table)

        logger.log("INFO", "phase3", "complete", len(fact_tables),
                   "All facts complete")

        # ── Phase 4: Atomic commit ─────────────────────────────────────────────
        _write_run_marker(run_id, "COMMITTED", list(all_reports.keys()), logger)
        _write_commit_marker(run_id, all_reports, logger)

        total_rows = sum(r.get("source_rows", 0) for r in all_reports.values())
        log_uri    = logger.flush_to_s3()

        _put_s3(
            {
                "run_id":    run_id,
                "job_name":  args["JOB_NAME"],
                "timestamp": datetime.utcnow().isoformat(),
                "status":    "SUCCESS",
                "totals": {
                    "tables_processed":     len(all_reports),
                    "source_rows":          total_rows,
                    "dimensions_processed": len(dimension_tables),
                    "facts_processed":      len(fact_tables),
                },
                "table_reports": all_reports,
                "log_file":      log_uri,
            },
            CONFIG["metadata"]["s3_bucket"],
            f"{CONFIG['metadata']['s3_prefix']}global_report_{run_id}.json",
        )

        logger.log("INFO", "job", "success", len(all_reports),
                   f"✓ JOB SUCCESS — {len(all_reports)} tables, {total_rows:,} rows",
                   extra={"run_id": run_id, "log_file": log_uri})
        job.commit()

    except Exception as exc:
        _write_run_marker(run_id, "FAILED", list(all_reports.keys()), logger)
        log_uri = logger.flush_to_s3()

        _put_s3(
            {
                "run_id":           run_id,
                "status":           "FAILED",
                "error":            str(exc),
                "error_type":       type(exc).__name__,
                "traceback":        traceback.format_exc(),
                "timestamp":        datetime.utcnow().isoformat(),
                "completed_tables": list(all_reports.keys()),
                "failed_at_table":  next(
                    (t for t in list(TABLE_CONFIGS.keys())
                     if t not in all_reports), "unknown"
                ),
                "log_file":         log_uri,
                "forensic_prefix":  (
                    f"s3://{CONFIG['errors']['s3_bucket']}/"
                    f"{CONFIG['errors']['s3_prefix']}"
                ),
            },
            CONFIG["metadata"]["s3_bucket"],
            f"{CONFIG['metadata']['s3_prefix']}failure_{run_id}.json",
        )
        raise

if __name__ == "__main__":
    main()