"""
Raw data ingestion for Pharma Data Platform.

Uploads CSVs from local directories to the S3 raw bucket with:
- MD5-based idempotency (skip unchanged files, error on silent corruption)
- Hive-style partitioning for fact tables
- Per-run JSON manifest uploaded to S3 on completion

Usage:
    python src/ingestion/upload_to_raw.py
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from aws.scripts.ingestion_utils import (
    generate_run_id,
    get_execution_identity,
    upload_file_with_tracking,
    create_manifest,
    upload_manifest,
)


BUCKET_NAME = "pharma-raw-dev"
SOURCE_SYSTEM = "ERP"
FORCE_OVERWRITE = False
DRY_RUN = False

RAW_DIR = Path("raw")
RAW_PARTITIONED_DIR = Path("raw_partitioned")


def discover_dimension_files():
    """
    Return upload configs for all dimension CSVs in raw/.

    S3 layout: dimensions/{table}/{file}.csv
    """
    dimension_files = [
        "customers.csv",
        "manufacturers.csv",
        "products_master.csv",
        "plant_warehouse.csv",
        "customer_plant_mapping.csv",
        "product_manufacturer_bridge.csv",
        "products_annual_demand.csv",
    ]

    configs = []
    for file_name in dimension_files:
        local_file = RAW_DIR / file_name
        if not local_file.exists():
            print(f"⚠️  Dimension file not found: {local_file}")
            continue

        table_name = file_name.removesuffix(".csv")
        configs.append({
            "local_file": str(local_file),
            "s3_key": f"dimensions/{table_name}/{file_name}",
            "table_name": table_name,
            "table_type": "dimension",
            "partition_year": None,
            "partition_month": None,
        })

    return configs


def discover_fact_files():
    """
    Return upload configs for all partitioned fact CSVs in raw_partitioned/.

    Expected local layout:
        raw_partitioned/{table}/year=YYYY/[month=MM]/{file}.csv

    S3 layout mirrors the local Hive-style partition paths:
        facts/{table}/year=YYYY/[month=MM]/{file}.csv
    """
    fact_tables = [
        "orders",
        "product_batches",
        "batch_allocator_central",
        "inventory_movements",
    ]

    configs = []
    for table in fact_tables:
        table_dir = RAW_PARTITIONED_DIR / table
        if not table_dir.exists():
            print(f"⚠️  Fact table directory not found: {table_dir}")
            continue

        for year_dir in table_dir.glob("year=*"):
            year = int(year_dir.name.split("=")[1])
            month_dirs = list(year_dir.glob("month=*"))

            if month_dirs:
                for month_dir in month_dirs:
                    month = int(month_dir.name.split("=")[1])
                    for csv_file in month_dir.glob("*.csv"):
                        configs.append({
                            "local_file": str(csv_file),
                            "s3_key": f"facts/{table}/year={year}/month={month:02d}/{csv_file.name}",
                            "table_name": table,
                            "table_type": "fact",
                            "partition_year": year,
                            "partition_month": month,
                        })
            else:
                for csv_file in year_dir.glob("*.csv"):
                    configs.append({
                        "local_file": str(csv_file),
                        "s3_key": f"facts/{table}/year={year}/{csv_file.name}",
                        "table_name": table,
                        "table_type": "fact",
                        "partition_year": year,
                        "partition_month": None,
                    })

    return configs


def run_ingestion():
    """
    Orchestrate file discovery, upload, and manifest creation.

    Returns:
        (run_id, manifest)
    """
    run_id = generate_run_id()
    executor_arn = get_execution_identity()
    run_start = datetime.now(timezone.utc)

    print("=" * 80)
    print("PHARMA DATA PLATFORM — RAW INGESTION")
    print(f"Run ID  : {run_id}")
    print(f"Started : {run_start.isoformat()}")
    print(f"Executor: {executor_arn}")
    print(f"Force   : {FORCE_OVERWRITE}  |  Dry run: {DRY_RUN}")
    print("=" * 80)
    print()

    files_to_upload = discover_dimension_files() + discover_fact_files()

    if DRY_RUN:
        files_to_upload = files_to_upload[:2]
        print(f"⚠️  DRY RUN: processing {len(files_to_upload)} files only\n")

    dim_count = sum(1 for f in files_to_upload if f["table_type"] == "dimension")
    fact_count = len(files_to_upload) - dim_count
    print(f"📋 Files to process: {len(files_to_upload)}  (dimensions: {dim_count}, facts: {fact_count})\n")

    upload_results = []
    stats = {"success": 0, "skipped": 0, "skipped_unverifiable": 0, "failed": 0}

    for file_config in files_to_upload:
        local_file = file_config["local_file"]

        if not Path(local_file).exists():
            print(f"⚠️  Local file not found: {local_file}")
            upload_results.append({
                "status": "failed",
                "event_type": "FAIL",
                "upload_record": {
                    "table_name": file_config["table_name"],
                    "table_type": file_config["table_type"],
                    "source_file_name": Path(local_file).name,
                    "s3_key": file_config["s3_key"],
                    "event_type": "FAIL",
                    "status": "failed",
                    "error_message": "Local file not found",
                },
            })
            stats["failed"] += 1
            continue

        result = upload_file_with_tracking(
            local_file=local_file,
            bucket=BUCKET_NAME,
            s3_key=file_config["s3_key"],
            table_name=file_config["table_name"],
            table_type=file_config["table_type"],
            partition_year=file_config.get("partition_year"),
            partition_month=file_config.get("partition_month"),
            run_id=run_id,
            executor_arn=executor_arn,
            source_system=SOURCE_SYSTEM,
            force=FORCE_OVERWRITE,
        )

        upload_results.append(result)
        stats[result["status"]] += 1
        print()

    run_end = datetime.now(timezone.utc)
    run_duration = (run_end - run_start).total_seconds()

    manifest = create_manifest(
        run_id=run_id,
        run_metadata={
            "run_id": run_id,
            "started_at": run_start.isoformat(),
            "completed_at": run_end.isoformat(),
            "duration_seconds": run_duration,
            "bucket": BUCKET_NAME,
            "source_system": SOURCE_SYSTEM,
            "force_overwrite": FORCE_OVERWRITE,
            "dry_run": DRY_RUN,
            "executor": executor_arn,
        },
        files_processed=[r["upload_record"] for r in upload_results],
        validation_results={
            "total_files": len(files_to_upload),
            **stats,
        },
        errors=[
            {
                "file": r["upload_record"]["source_file_name"],
                "table": r["upload_record"]["table_name"],
                "error": r.get("error", r["upload_record"].get("error_message")),
            }
            for r in upload_results if r["status"] == "failed"
        ],
    )

    upload_manifest(BUCKET_NAME, manifest, f"_metadata/manifests/manifest_{run_id}.json")

    print()
    print("=" * 80)
    print("INGESTION COMPLETE")
    print(f"Run ID   : {run_id}")
    print(f"Duration : {run_duration:.2f}s")
    print(f"Total    : {len(files_to_upload)}")
    print(f"  ✅ Success            : {stats['success']}")
    print(f"  ⏭️  Skipped            : {stats['skipped']}")
    print(f"  ⚠️  Skipped (unverif.) : {stats['skipped_unverifiable']}")
    print(f"  ❌ Failed             : {stats['failed']}")
    print("=" * 80)

    return run_id, manifest


if __name__ == "__main__":
    try:
        run_id, manifest = run_ingestion()
        failed = manifest["validation_results"]["failed"]
        if failed:
            print(f"\n❌ Ingestion completed with {failed} failure(s)")
            sys.exit(1)
        print("\n✅ Ingestion completed successfully")
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted — already-uploaded files will be skipped on next run")
        sys.exit(130)

    except Exception as e:
        print(f"\n❌ FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)