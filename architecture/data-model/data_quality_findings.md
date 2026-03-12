# Data Quality Findings & Accepted Exceptions

**Phase:** Phase 1 — Pre-Ingestion Data Profiling  
**Scope:** 11 Source Tables (2022–2025)  
**Validation Coverage:** 23 Automated Checks
**Validation Execution:** Automated via `src/profiling/model_validation.py` (aggregated cross-table validation framework)
**Result:** Approved for Phase 2 Ingestion (22/23 Passed, 1 Accepted Exception)

---

# Validation Framework

Data quality validation covered:

- Foreign Key Integrity
- Grain Enforcement
- Cross-Year Schema Consistency
- Null Pattern Analysis
- Business Logic Validation

Severity classification:

| Level | Meaning |
|-------|---------|
| P0 | Blocking — ingestion cannot proceed |
| P1 | Non-blocking — documented & accepted |
| P2 | Minor — informational |

**No P0 issues identified.**

---

# Key Findings (P1 — Non-Blocking)

## 1. Product Code Non-Uniqueness (`products_master`)

`product_code` is not unique.  
Multiple SKUs (different pack sizes) share the same code.

**Impact:**  
Using `product_code` as a join key would create fan-out duplicates.

**Decision:**  
- `product_id` enforced as the only valid FK reference.
- `product_code` reclassified as product family identifier.
- Composite lookup (if required): `product_code + pack_size_label`.

---

## 2. Cold Storage Capacity Mismatch (`plant_warehouse`)

3 plants contain:

- `cold_storage_available = 'No'`
- `cold_storage_capacity > 0`

**Impact:**  
Potential misclassification in cold-chain allocation logic.

**Decision:**  
Accepted as source-of-truth. Flagged for curated-zone annotation.

---

## 3. Strength Field Inconsistency (`products_master`)

Liquid formulations store concentration in combined format  
(e.g., `"250mg/5ml"`) with `strength_unit = NULL`.

**Impact:**  
Numeric strength comparison unreliable.

**Decision:**  
Parse into derived numeric + unit columns in curated zone.

---

# Accepted Business Logic Exception

## Reversed Invoice Dates on Returns (`orders_erp`)

1.06% of rows show:

`invoice_date < actual_delivery_date`

**Root Cause:**  
Credit memo (return invoice) issued before physical return is logged.

**Decision:**  
Accepted — correct ERP behavior.  
Return events modeled as first-class lifecycle facts.

---

# Null Pattern Summary

| Table | Column | Null % | Interpretation |
|--------|--------|--------|----------------|
| `orders_erp` | `batch_id` | ~66% | Populated only post-delivery |
| `orders_erp` | `invoice_date` | ~34% | Only invoiced lines |
| `inventory_movements` | `allocation_id` | ~70% | Optional linkage |
| `products_master` | `strength_unit` | ~21% | Liquid concentration formatting |

All mandatory FK columns validated at 100% population.

---

# Referential Integrity Summary

- Total FK relationships validated: 21  
- Mandatory FKs: 100% resolved  
- Conditional FKs: Valid where populated  

No orphan records detected.

---

# Cross-Year Consistency (2022–2025)

- No schema drift across annual ERP exports  
- No customer or plant disappearance mid-period  
- Product expansion observed (expected business growth)

Single schema definition supports all four years.

---

# Validation Summary

| Category | Passed | Failed |
|-----------|--------|--------|
| FK Integrity | 10 | 0 |
| Grain Validation | 4 | 0 |
| Cross-Year Checks | 3 | 0 |
| Null Analysis | 2 | 0 |
| Business Logic | 3 | 0 (1 accepted anomaly) |
| **Total** | **22** | **0** |

---

# Phase 1 Decision

All source datasets cleared for S3 raw-zone ingestion.

Design decisions made during profiling directly informed:
- Surrogate key enforcement
- Event-level fact modeling
- Bridge table implementation
- Join-key constraints

Phase 2 ingestion approved.