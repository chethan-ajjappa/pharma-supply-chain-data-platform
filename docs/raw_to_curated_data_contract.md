# Data Contract: Raw to ETL Layer

**Purpose:** Define schema baseline, type conversion rules, and data quality constraints for ETL 
**Database:** `raw_zone` (Glue Data Catalog)   
**Status:**  Catalog for ETL consumption

---

## Overview

This document defines the contract between the raw data layer and the ETL layer.

- Source data stored as CSV in S3 (`pharma-raw-dev`)
- Tables registered in Glue Catalog via crawlers
- Schema is inferred (schema-on-read) and is predominantly `STRING`
- No type enforcement is applied in the raw layer

The contract specifies:

- Expected target schema (post-transformation types)
- Type conversion rules
- Data quality thresholds
- Failure vs warning conditions

---

## Design Principles

- **Separation of concerns**
  - Raw layer: ingestion only (no transformation)
  - ETL layer: schema enforcement and validation

- **Schema handling**
  - Raw data is stored without enforced schema (CSV in S3)
  - Glue Catalog provides a query schema for Athena (inferred, not authoritative)
  - Actual type enforcement happens only during ETL

- **No implicit data loss**
  - Type casting is deferred to ETL where failures are controlled and measurable

- **Deterministic transformations**
  - All casting, validation, and corrections are explicit and auditable

---

## Catalog Summary

| Category | Tables | Partitions |
|---|---|---|
| Dimensions | 7 | 0 |
| Facts | 4 | 60 |
| **Total** | **11** | **60** |

### Partition Distribution

| Table | Partition Keys | Count | Notes |
|---|---|---|---|
| `orders` | year, month | 48 | Monthly partitioning for high-volume data (~3.6M rows) |
| `inventory_movements` | year | 4 | Annual partitioning (~1M rows) |
| `product_batches` | year | 4 | Low volume (~23K rows) |
| `batch_allocator_central` | year | 4 | Moderate volume (~94K rows) |

---

## Raw Layer Characteristics

- Source format: CSV (no native typing)
- Schema inferred via Glue Crawlers
- Columns predominantly `STRING`
- No type enforcement at ingestion

**Implication:**
- Column structure is authoritative
- Data types are not reliable
- All casting and validation handled in ETL

---

## Schema Baseline — Dimensions

### `customers`

| Column | Source Type | Target Type |
|---|---|---|
| customer_id | STRING | STRING  |
| customer_code | STRING | STRING |
| customer_name_desc | STRING | STRING |
| customer_type | STRING | STRING |
| region | STRING | STRING |
| ownership_type | STRING | STRING |
| contract_start_date | STRING | DATE |
| contract_end_date | STRING | DATE |

**Primary key:** `customer_id`  
**Business key:** `customer_code`

**Rules:**
- `customer_id` must be non-null
- `contract_end_date` is nullable (active contracts)

---

### `plant_warehouse`

| Column | Source Type | Target Type |
|---|---|---|
| plant_id | STRING | INT |
| plant_code | STRING | STRING |
| plant_type | STRING | STRING |
| latitude | STRING | DOUBLE |
| longitude | STRING | DOUBLE |
| cold_storage_capacity | STRING | INT |
| parent_plant_code | STRING | STRING |

**Primary key:** `plant_id`

**Rules:**
- Latitude/longitude must be valid numeric values
- `parent_plant_code` is NULL for central distribution centers
- Data inconsistency: some rows have `cold_storage_available = No` but capacity > 0 → flag in ETL

---

### `products_master`

| Column | Source Type | Target Type |
|---|---|---|
| product_id | STRING | INT |
| productcode | STRING | STRING |
| price | STRING | DECIMAL(18,2) |
| gst_rate | STRING | DECIMAL(5,2) |
| coldchainrequired | STRING | BOOLEAN |
| isessentialdrug | STRING | BOOLEAN |
| is_discontinued | STRING | BOOLEAN |
| pack_count | STRING | INT |
| shelflife_months | STRING | INT |
| strength_value | STRING | STRING |

**Primary key:** `product_id`

**Critical Rules:**
- `productcode` is NOT unique → never use for joins
- Boolean fields use 0/1 pattern → must cast via INT → BOOLEAN
- `pack_count` contains known invalid values → tolerate and log

---

### `manufacturers`

| Column | Source Type | Target Type |
|---|---|---|
| manufacturer_id | STRING | STRING |
| manufacturer_name | STRING | STRING |

No known issues.

---

### `customer_plant_mapping`

| Column | Source Type | Target Type |
|---|---|---|
| mapping_id | STRING | STRING |
| customer_id | STRING | STRING |
| plant_code | STRING | STRING |
| priority_flag | STRING | STRING |
| active_flag | STRING | BOOLEAN |

**Grain:** One row per (customer × plant)

---

### `product_manufacturer_bridge`

| Column | Source Type | Target Type |
|---|---|---|
| product_id | STRING | INT |
| manufacturer_id | STRING | INT |
| production_level | STRING | STRING |
| price | STRING | DECIMAL(10,2) |

**Grain:** One row per (product × manufacturer)
**Primary key:** Composite — (manufacturer_id, product_id)  
No surrogate key exists. Neither column alone is unique.

---

### `products_annual_demand`

| Column | Source Type | Target Type |
|---|---|---|
| product_id | STRING | INT |
| annual_target_units | STRING | INT |
| batches_per_year | STRING | INT |
| nominal_batch_size | STRING | INT |
| unit_mrp | STRING | DECIMAL(10,2) |

**Note:**
- Demand data is independent of manufacturing data → no reconciliation required

---
## Schema Baseline — Facts

**Processing order (upstream → downstream):**
`product_batches` → `batch_allocator_central` → `inventory_movements` → `orders`

---

### `product_batches`

| Column | Source Type | Target Type |
|---|---|---|
| batch_id | STRING | STRING |
| product_id | STRING | INT |
| manufacturer_id | STRING | INT |
| mfg_date | STRING | DATE |
| expiry_date | STRING | DATE |
| batch_size_units | STRING | INT |
| unit_mrp | STRING | DECIMAL(18,2) |
| coldchainrequired | STRING | BOOLEAN |

**Rules:**
- `expiry_date` must be greater than `mfg_date`
- All IDs must be non-null and valid

---

### `batch_allocator_central`

| Column | Source Type | Target Type |
|---|---|---|
| allocation_id | STRING | STRING |
| batch_id | STRING | STRING |
| product_id | STRING | INT |
| manufacturer_id | STRING | INT |
| plant_id | STRING | INT |
| units_allocated | STRING | INT |
| allocation_date | STRING | DATE |
| coldchainrequired | STRING | BOOLEAN |

**Rules:**
- All IDs must be non-null and valid
- `units_allocated` must be positive

---

### `inventory_movements`

| Column | Source Type | Target Type |
|---|---|---|
| movement_id | STRING | STRING |
| movement_type | STRING | STRING |
| batch_id | STRING | STRING |
| from_plant_id | STRING | INT |
| to_plant_id | STRING | INT |
| units | STRING | INT |
| movement_date | STRING | DATE |
| allocation_id | STRING | STRING |
| coldchainrequired | STRING | BOOLEAN |

**Rules:**
- Movement direction derived from `movement_type`
- Null constraints:
  - `IN` → `from_plant_id` must be NULL
  - `TRANSFER` → both plants must be present
- `units` must be positive

---

### `orders` - Lifecycle event fact

| Column | Source Type | Target Type |
|---|---|---|
| erp_export_row_id | STRING | STRING |
| order_line_id | STRING | STRING |
| customer_id | STRING | STRING |
| product_id | STRING | INT |
| plant_code | STRING | STRING |
| batch_id | STRING | STRING |
| order_date | STRING | DATE |
| actual_delivery_date | STRING | DATE |
| invoice_date | STRING | DATE |
| order_qty | STRING | INT |
| order_price | STRING | DECIMAL(18,2) |
| gst_amount | STRING | DECIMAL(18,2) |
| line_status | STRING | STRING |
| order_year | STRING | INT |
| rma_no | STRING | STRING |

**Primary key:** `erp_export_row_id`
**Grain:** One row per ERP export event per order line.  
A single order_line_id has up to 5 rows — one per lifecycle stage  
(Ordered, Delivered, Invoiced, Cancelled, Returned).

**Rules:**
- `customer_id`, `product_id` are mandatory foreign keys
- `batch_id` is optional (~33% NULL expected)
- Date columns are conditionally populated based on lifecycle stage
- Schema drift:
  - `rma_no` missing in 2022 → must be added as NULL

**CRITICAL:**
- Do not aggregate `order_qty`, `delivery_qty`, `invoice_qty` without filtering `line_status`
- Each lifecycle stage generates separate rows → aggregation must filter by `line_status`

---

## Type Conversion Strategy

All type conversions are explicit and validated in ETL. No silent coercion.

| Column Pattern | Cast | On Failure | Threshold | Action |
|---|---|---|---|---|
| Numeric system IDs (plant_id, product_id, hsncode) | STRING → INT | Set NULL | 0% | FAIL |
| Business / ERP keys (customer_id, manufacturer_id, batch_id, allocation_id, movement_id, erp_export_row_id) | STRING → STRING | Trim + empty → NULL | 0% | FAIL |
| Date columns | STRING → DATE | Set NULL | 2% | WARN ≤2%, FAIL otherwise |
| Boolean flags (0/1) | STRING → INT → BOOLEAN | Set NULL | 0% | FAIL |
| Boolean flags Yes/No | STRING → BOOLEAN | Set NULL | 0% | FAIL |
| Numeric measures | STRING → DECIMAL | Set NULL | 1% | WARN ≤1%, FAIL otherwise |
| `pack_count` | STRING → INT | Set NULL | 2% | WARN (known issue) |

**Boolean casting pattern:**
```python

col("coldchainrequired").cast("int").cast("boolean")

```

**Date parsing:**
```python

to_date(col("order_date"), "yyyy-MM-dd")

```

---

## ETL Quality Gates

### Fail Conditions

| Check | Condition |
|---|---|
| Row count drop | Any rows lost (output < source count) |
| Null in mandatory ID | > 0% |
| Missing expected columns | Any |
| ID cast failure | > 0% |
| Invalid business rule | `expiry_date < mfg_date` |

### Warning Conditions

| Check | Condition |
|---|---|
| Date parse failures | ≤ 2% |
| Partition mismatch | ≤ 1% |
| Invalid `pack_count` | ≤ 2% |
| New columns detected | Log and include |

---
## Row Count Validation

### Dimensions (Strict Match)

| Table | Expected Rows |
|---|---|
| customers | 2,500 |
| manufacturers | 10 |
| customer_plant_mapping | 3,384 |
| plant_warehouse | 57 |
| products_master | 702 |
| products_annual_demand | 702 |
| product_manufacturer_bridge | 702 |

### Facts (Full Scan Required)

| Table | Expected Rows |
|---|---|
| orders | 3,596,681 |
| inventory_movements | 1,042,914 |
| product_batches | 23,658 |
| batch_allocator_central | 94,306 |

**Validation rule:**  
Zero row loss tolerance. Output row count must equal source row count exactly.

---

## Schema Evolution

| Table Type | Behavior | Handling |
|---|---|---|
| Facts | New columns may appear | Add as NULL (union schema) |
| Dimensions | Stable schema | Unexpected columns → FAIL |

---

## Catalog Guarantees

### Provided

- Tables are queryable via Glue/Athena  
- Partition structure follows Hive format  
- Dimension schemas are stable  
- Row counts are baselined  
- Partition pruning is effective for Athena queries  

### Not Provided

- Correct data types (STRING-heavy by design)  
- Data quality guarantees  
- Schema stability in fact tables  
- Automatic discovery of new partitions  

---

## Partition Management

| Scenario | Action |
|---|---|
| New partition (same schema) | `MSCK REPAIR TABLE` |
| Schema change | Re-run crawler |
| Initial setup | Crawler |

**Rule:**  
Use MSCK for partition updates. Use crawlers only for schema changes.