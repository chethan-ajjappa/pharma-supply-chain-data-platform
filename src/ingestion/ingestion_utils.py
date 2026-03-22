"""
Ingestion utilities for Pharma Data Platform.

Handles file upload to S3 with MD5-based idempotency, multipart awareness,
and per-file event tracking for manifest generation.
"""

import boto3
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import uuid
from botocore.exceptions import ClientError


STATUS_SUCCESS = "success"
STATUS_SKIPPED = "skipped"
STATUS_SKIPPED_UNVERIFIABLE = "skipped_unverifiable"
STATUS_FAILED = "failed"

EVENT_UPLOAD = "UPLOAD"
EVENT_SKIP = "SKIP"
EVENT_FAIL = "FAIL"

VERIFY_MD5 = "md5"
VERIFY_UNVERIFIABLE = "unverifiable"
VERIFY_NONE = "none"


def generate_run_id(prefix="run"):
    """
    Generate a unique ingestion run ID.

    Format: run-YYYYMMDD-HHMMSS-<8-char uuid>
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    short_uuid = str(uuid.uuid4())[:8]
    return f"{prefix}-{timestamp}-{short_uuid}"


def get_execution_identity():
    """
    Return the IAM ARN of the current AWS caller.

    Call once per run and pass as a parameter to avoid STS throttling.
    """
    sts = boto3.client("sts")
    return sts.get_caller_identity()["Arn"]


def calculate_md5(file_path):
    """Return the MD5 hex digest of a local file (streamed in 64 KB chunks)."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5.update(chunk)
    return md5.hexdigest()


def get_file_size(file_path):
    """Return file size in bytes."""
    return Path(file_path).stat().st_size


def get_s3_object_metadata(bucket, key):
    """
    Return ETag, size, and multipart flag for an S3 object.

    Returns None if the object does not exist.
    Raises RuntimeError on any non-404 S3 error.

    Multipart uploads produce ETags of the form '<md5>-<part_count>',
    which are not comparable to a local MD5.
    """
    s3 = boto3.client("s3")
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        etag = response["ETag"].strip('"')
        return {
            "etag": etag,
            "size": response["ContentLength"],
            "is_multipart": "-" in etag,
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None
        raise RuntimeError(f"Failed to get S3 object metadata: {e}")


def should_skip_file(bucket, s3_key, local_checksum, force=False):
    """
    Decide whether to skip, upload, or raise on a given file.

    Returns:
        (should_skip, reason, verification_method, s3_metadata)

    Raises:
        RuntimeError: Single-part ETag exists but does not match local MD5
                      and force=False. Requires manual investigation.
    """
    s3_metadata = get_s3_object_metadata(bucket, s3_key)

    if s3_metadata is None:
        return (False, "file_not_exists", VERIFY_NONE, None)

    if s3_metadata["is_multipart"]:
        if force:
            return (False, "force_overwrite_multipart", VERIFY_UNVERIFIABLE, s3_metadata)
        return (True, "multipart_unverifiable", VERIFY_UNVERIFIABLE, s3_metadata)

    s3_checksum = s3_metadata["etag"]

    if s3_checksum == local_checksum:
        if force:
            return (False, "force_overwrite", VERIFY_MD5, s3_metadata)
        return (True, "checksum_match", VERIFY_MD5, s3_metadata)

    if force:
        return (False, "force_overwrite_mismatch", VERIFY_MD5, s3_metadata)

    raise RuntimeError(
        f"CHECKSUM MISMATCH: s3://{bucket}/{s3_key}\n"
        f"  S3 ETag:    {s3_checksum}\n"
        f"  Local MD5:  {local_checksum}\n"
        f"  S3 size:    {s3_metadata['size']} bytes\n\n"
        f"  Possible causes: S3 corruption, local file modified after upload, "
        f"or upstream data change.\n"
        f"  Use force=True to explicitly overwrite."
    )


def create_manifest(run_id, run_metadata, files_processed, validation_results=None, errors=None):
    """Assemble and return the ingestion manifest dict."""
    return {
        "manifest_version": "1.0",
        "ingestion_run_id": run_id,
        "run_metadata": run_metadata,
        "files_processed": files_processed,
        "validation_results": validation_results or {},
        "errors": errors or [],
    }


def upload_manifest(bucket, manifest, manifest_key):
    """Serialize manifest to JSON and upload to S3."""
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
    )
    print(f"✅ Manifest uploaded: s3://{bucket}/{manifest_key}")


def upload_file_with_tracking(
    local_file,
    bucket,
    s3_key,
    table_name,
    table_type,
    partition_year=None,
    partition_month=None,
    run_id=None,
    executor_arn=None,
    source_system="ERP",
    force=False,
):
    """
    Upload a single file to S3 with idempotency checks and full event tracking.

    Verification method is set to 'unverifiable' for multipart uploads (where
    the S3 ETag is not a plain MD5) and 'md5' for single-part uploads.

    Args:
        local_file: Path to the local CSV file.
        bucket: Target S3 bucket.
        s3_key: Target S3 key.
        table_name: Logical table name (used in manifest).
        table_type: "dimension" or "fact".
        partition_year: Year partition label (facts only).
        partition_month: Month partition label (facts only).
        run_id: Ingestion run ID generated by generate_run_id().
        executor_arn: IAM ARN from get_execution_identity() — pass once per run.
        source_system: Source system tag written to the manifest record.
        force: If True, overwrite existing files regardless of checksum.

    Returns:
        dict with keys: status, event_type, upload_record, and either
        duration (success), reason (skip), or error (fail).
    """
    start_time = datetime.now(timezone.utc)
    local_checksum = calculate_md5(local_file)
    file_size = get_file_size(local_file)

    upload_record = {
        "ingestion_run_id": run_id,
        "table_name": table_name,
        "table_type": table_type,
        "partition_year": partition_year,
        "partition_month": partition_month,
        "source_file_name": Path(local_file).name,
        "source_system": source_system,
        "s3_key": s3_key,
        "file_size_bytes": file_size,
        "md5_checksum": local_checksum,
        "event_type": None,
        "status": "pending",
        "error_message": None,
        "verification_method": None,
        "is_multipart": None,
        "s3_etag": None,
        "upload_timestamp": start_time.isoformat(),
        "upload_duration_seconds": 0,
        "execution_identity": executor_arn,
    }

    # Idempotency check
    try:
        should_skip, reason, verification_method, s3_metadata = should_skip_file(
            bucket=bucket,
            s3_key=s3_key,
            local_checksum=local_checksum,
            force=force,
        )

        if should_skip:
            end_time = datetime.now(timezone.utc)
            status = STATUS_SKIPPED_UNVERIFIABLE if verification_method == VERIFY_UNVERIFIABLE else STATUS_SKIPPED
            print(f"  ⏭️  Skipping {s3_key} ({reason})")

            upload_record.update({
                "event_type": EVENT_SKIP,
                "status": status,
                "error_message": reason,
                "verification_method": verification_method,
                "is_multipart": s3_metadata["is_multipart"] if s3_metadata else None,
                "s3_etag": s3_metadata["etag"] if s3_metadata else None,
                "upload_timestamp": end_time.isoformat(),
                "upload_duration_seconds": (end_time - start_time).total_seconds(),
            })

            return {
                "status": status,
                "event_type": EVENT_SKIP,
                "reason": reason,
                "s3_metadata": s3_metadata,
                "upload_record": upload_record,
            }

    except RuntimeError as e:
        end_time = datetime.now(timezone.utc)
        upload_record.update({
            "event_type": EVENT_FAIL,
            "status": STATUS_FAILED,
            "error_message": str(e),
            "verification_method": VERIFY_MD5,
            "upload_timestamp": end_time.isoformat(),
            "upload_duration_seconds": (end_time - start_time).total_seconds(),
        })
        print(f"❌ {e}")
        return {"status": STATUS_FAILED, "event_type": EVENT_FAIL, "error": str(e), "upload_record": upload_record}

    # Upload
    s3_client = boto3.client("s3")
    try:
        print(f"⬆️  Uploading {s3_key} ({file_size / 1024 / 1024:.2f} MB)...")
        s3_client.upload_file(Filename=local_file, Bucket=bucket, Key=s3_key)

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        s3_meta = get_s3_object_metadata(bucket, s3_key)
        verification_method = VERIFY_UNVERIFIABLE if s3_meta["is_multipart"] else VERIFY_MD5

        upload_record.update({
            "event_type": EVENT_UPLOAD,
            "status": STATUS_SUCCESS,
            "error_message": None,
            "verification_method": verification_method,
            "is_multipart": s3_meta["is_multipart"],
            "s3_etag": s3_meta["etag"],
            "upload_timestamp": end_time.isoformat(),
            "upload_duration_seconds": duration,
        })

        print(f"✅ Uploaded {s3_key} ({file_size / 1024 / 1024:.2f} MB in {duration:.2f}s)")
        return {"status": STATUS_SUCCESS, "event_type": EVENT_UPLOAD, "duration": duration, "upload_record": upload_record}

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        upload_record.update({
            "event_type": EVENT_FAIL,
            "status": STATUS_FAILED,
            "error_message": str(e),
            "upload_timestamp": end_time.isoformat(),
            "upload_duration_seconds": (end_time - start_time).total_seconds(),
        })
        print(f"❌ Failed to upload {s3_key}: {e}")
        return {"status": STATUS_FAILED, "event_type": EVENT_FAIL, "error": str(e), "upload_record": upload_record}