# AWS Infrastructure

**Region:** us-east-1

---

## Storage Architecture (S3)

| Layer | Bucket | Purpose |
|------|--------|--------|
| Raw | `pharma-raw-dev` | Immutable source data (CSV) |
| Curated | `pharma-curated-dev` | Cleaned + transformed (Parquet) |
| Warehouse | `pharma-warehouse-dev` | Analytics-ready tables |
| Query Results | `pharma-athena-results-dev` | Athena outputs |

### Design Decisions

- Versioning **enabled only on RAW**
  → preserves source-of-truth integrity

- Derived layers are **reproducible**
  → versioning intentionally disabled

- Encryption: **SSE-S3 (AES256)** across all buckets

---

## Compute Access (IAM)

**Role:** `GlueServiceRole`  
**Used by:** AWS Glue

### Permissions

- S3 (scoped to pharma buckets)
- Glue Data Catalog
- CloudWatch Logs

---

## Query Layer (Athena)

- Output bucket: `pharma-athena-results-dev`
- Isolated from data layers

**Why:**
- avoids data pollution  
- simplifies cleanup and cost tracking  

---