# Raw Ingestion Strategy

**Target:** `s3://pharma-raw-dev/`  
**Script:** `src/ingestion/upload_to_raw.py`

---

## What This Pipeline Does

- Ingests **11 CSV source tables** into S3 raw zone
- Preserves **original format (no transformation)**
- Applies **partitioned folder structure**
- Ensures **idempotent uploads (no duplicates)**
- Generates **run-level audit manifest**

---

## Key Engineering Decisions

- **Pre-partitioning before upload**
  - Orders split monthly
  - Other facts split yearly
  - Reduces downstream Glue scan size significantly

- **Idempotent uploads**
  - MD5 vs S3 ETag comparison
  - Prevents duplicate or partial re-uploads

- **Manifest-based auditing**
  - One JSON per run
  - Tracks file-level ingestion status

---

## Data Layout (Raw Zone)

```
s3://pharma-raw-dev/
│
├── dimensions/
├── facts/
│ ├── orders/year=YYYY/month=MM/
│ ├── product_batches/year=YYYY/
│ ├── batch_allocator_central/year=YYYY/
│ └── inventory_movements/year=YYYY/
│
└── _metadata/manifests/
```

---

## Ingestion Scale

- **Total dataset size:** ~1.6 GB  
- **Total files:** 67  
- **Total tables:** 11  

**Orders (largest table):**
- ~3.6M rows  
- ~1.3 GB (partitioned monthly)

**Other tables (10 tables combined):**
- ~1.1M rows   

**End-to-end ingestion time:** ~15 minutes

---

## Sample Run Output

Run ID: run-20260108-024931
Duration: ~15 minutes
Files processed: 67

Success: 67
Skipped (duplicate): 0
Skipped (unverifiable): 0
Failed: 0

**Execution Details:**
- Source: ERP
- Executor: IAM user (tracked per run)
- Multipart uploads handled (large files)

---

## Manifest Snapshot

Each ingestion run generates a structured JSON manifest:

```json
{
  "run_id": "run-20260108-024931",
  "duration_seconds": 895,
  "source_system": "ERP",
  "validation_results": {
    "total_files": 67,
    "success": 67,
    "failed": 0
  }
}
```

**Note:**  
- File-level tracking includes checksum validation (MD5 vs S3 ETag)
- Multipart uploads handled explicitly (ETag not equal to MD5)
- Files >5MB use multipart upload → checksum verification adjusted accordingly

---

## Validation

- Verified S3 structure via AWS CLI
- Partition paths validated
- Screenshot: Partitioned orders data (year-level view showing monthly partitions)

![S3 Partition Structure](/outputs/screenshots/s3_orders_partitions.png)

---

## Idempotency Design

- Each file is validated before upload using MD5 checksum
- Compared against S3 object ETag (for single-part uploads)

| Condition               | Action     | Event Type           |
| ----------------------- | ---------- | -------------------- |
| Object does not exist   | Upload     | NEW_UPLOAD           |
| Hash matches            | Skip       | SKIPPED_DUPLICATE    |
| Hash differs            | Skip + log | SKIPPED_UNVERIFIABLE |
| Force overwrite enabled | Upload     | FORCE_OVERWRITE      |

Result:

- Re-run after full success → no-op
- Re-run after failure → only missing files uploaded

---

## Pre-Upload Processing

* Orders data split into **monthly partitions**
* Other fact tables split into **year-level partitions**
* Output structured in Hive-style layout (`year=YYYY/month=MM`)

**Impact:**

* Reduces downstream processing scope
* Enables efficient incremental loads
* Avoids full dataset reprocessing in Glue

---

## Audit Manifest

* One JSON manifest generated per ingestion run
* Stored under `_metadata/manifests/` in S3

**Captures:**

* Run metadata (duration, executor, source)
* File-level status (success, skipped, failed)
* Validation summary

**Design Choice:**

* Structured JSON instead of logs → directly queryable and self-contained