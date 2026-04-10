# ETL Architecture

**Layer:** Raw (CSV) → Curated (Parquet)  
**Compute:** AWS Glue (PySpark)  
**Pattern:** Batch ETL with fail-fast data quality checks

---

## Job Design

Two Glue jobs handle transformation:

| Job | Tables | Volume | Notes |
|---|---|---|---|
| `etl_orders.py` | Orders (lifecycle event fact) | ~3.6M rows | Event grain: one row per lifecycle stage per order line. A single order_line_id has up to 5 rows — Ordered, Delivered, Invoiced, Cancelled, Returned. Never aggregate qty columns without filtering on line_status. |
| `etl_multi_table.py` | 7 dimensions + 3 facts | ~1.1M rows | Dimensions first, then facts |

### Design Rationale

- **Orders isolated:**  
  High volume and business-critical; separate job allows independent retries and tuning.

- **Grouped multi-table job:**  
  Dimensions and dependent facts processed together to avoid partial states.

- **Avoided per-table jobs:**  
  unnecessary orchestration overhead for small tables.

---

## Data Flow

```
S3 Raw Zone (CSV)
    │
    ├── Glue Catalog (schema-on-read, raw_zone database)
    │       └── Provides S3 table locations and partition metadata
    │
    ├── Job 1: etl_orders.py
    │       read S3 (header=true) → validate → transform → quality gates → write Silver
    │       Partition: year / month
    │
    └── Job 2: etl_multi_table.py
            Phase 2: 7 dimensions (sequential, abort on first failure)
            Phase 3: 3 facts (only after all dimensions succeed)
            Partition: year (facts only)
                │
                └── S3 Curated Zone (Parquet · partitioned)
```

---
 
## Source Read Strategy
 
Both jobs use the same approach:
 
1. Resolve S3 location from Glue Catalog API (`glue_client.get_table`)
2. Read CSV directly from S3 using `spark.read.csv` with `header=true`
3. Normalize column names to lowercase with underscores
 
Reading with `header=true` lets Spark handle the header row automatically — no post-read filter needed. This avoids the header-leak problem that occurs when reading partitioned CSVs via `spark.table()`.

---

## Processing Strategy

**Column normalization**  
All column names standardized to lowercase with underscores on read. Eliminates mismatches between source naming and target schema.
 
**Schema enforcement**  
Raw data ingested as STRING. Explicit target schemas applied during transformation.
 
**Boolean casting**  
Source stores boolean values as `"0"`/`"1"`. Two-step cast: column → INT → BOOLEAN. Direct `cast("boolean")` on string `"0"`/`"1"` returns NULL incorrectly.
 
**Schema drift (orders `rma_no`)**  
Nullable field in target schema handles partitions where the column is absent.

---

## Write Strategy

- Output format: **Parquet**
- Partitioning:
  - Orders → `year/month`
  - Facts → `year`
- **Dynamic partition overwrite** — only affected partitions replaced; safe re-runs

---

## Data Quality

The pipeline follows a fail-fast approach:
- No data is written if critical validations fail
- Validation includes primary key integrity, schema enforcement, and partition consistency

Checks enforced during ETL before writing to curated layer. Fail-fast — no data written if critical checks fail.
 
See `docs/data_quality_framework.md` for rules and thresholds.

---

## Idempotency & Run Control

- Re-runs produce consistent outputs (no duplication)
- Multi-table job uses S3 run markers: `IN_PROGRESS` → `COMMITTED` / `FAILED`
- Prevents concurrent writes to curated layer

---

## Output Locations
 
| Purpose | Path |
|---|---|
| Curated data | `s3://pharma-curated-dev/` |
| ETL metadata | `s3://pharma-curated-dev/etl-metadata/` |
| Error / reject data | `s3://pharma-curated-dev/etl-errors/` |
