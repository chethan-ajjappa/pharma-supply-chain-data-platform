# Glue Catalog Design
**Scope:** Metadata layer for raw data access  
**Type:** Architecture + operational decisions  

---

## Overview

The Glue Catalog provides a metadata layer over raw S3 data, enabling structured access for ETL and query engines.

* Raw data is stored in CSV format in S3 
* Tables were initially discovered using crawlers and are now managed explicitly via Athena DDL. 
* Athena and ETL jobs read from the same catalog 
* Catalog defines table structure and partitions, but does not guarantee data correctness

The goal is to make raw data discoverable and queryable without hardcoding S3 paths.

---

## Usage in This Architecture

Glue Catalog is used for:
- Table discovery
- S3 location resolution
- Partition metadata

Data reading is performed using Spark with explicit control over schema and parsing.
Catalog schema is treated as advisory and not enforced during ETL.

---

## Why Use Glue Catalog

Without a catalog, ETL jobs read directly from S3:

```python
df = spark.read.csv("s3://pharma-raw-dev/facts/orders/year=2022/month=01/")
```

**Limitations:**
*  Tight coupling to S3 paths 
*  Manual partition handling 
*  Schema inferred repeatedly at runtime 
*  No shared metadata layer for querying

**Benefits:**
* Decouples compute from storage paths
* Enables partition discovery
* Provides consistent metadata across tools 
* Supports Athena-based querying

---

## Crawler Strategy

### Initial Setup

A single crawler over the full S3 prefix resulted in:

* Table merging due to overlapping column names
* Incorrect schema inference across unrelated datasets

### Final Design

* One crawler for all dimension tables
* One crawler per fact table:

  * `orders`
  * `inventory_movements`
  * `product_batches`
  * `batch_allocator_central`

**Outcome:**

* 11 tables created correctly
* 60 partitions discovered
* No schema conflicts

## Schema Management Strategy

Crawlers were used only during initial data discovery to understand dataset structure.

After initial setup:
- Tables are created and managed using Athena DDL
- Schema definitions are explicitly controlled
- Crawlers are not used in ongoing pipeline operations

This avoids:
- Uncontrolled schema changes
- Inconsistent type inference
- Unexpected downstream breakages

## Key Observations

* Crawlers assume homogeneous schema within a prefix
* Multi-table prefixes lead to incorrect table inference
* Splitting crawlers by dataset is required for heterogeneous data

---

## Raw Zone Design

* Data stored as-is in CSV format
* No schema enforcement at ingestion 
* Catalog schema is inferred and predominantly STRING

**Implication:**

* Catalog provides structural metadata only
* Data types are not reliable
* Type casting and validation are deferred to ETL

---

## Case Sensitivity

* Glue Catalog stores column names in lowercase
* Profiling outputs used mixed/uppercase names

**Handling:**

* Normalize column names to lowercase during validation
* Treat catalog schema as authoritative for column naming

---


## Partition Management

| Scenario                   | Action              |
| -------------------------- | ------------------- |
| New partition added        | `MSCK REPAIR TABLE` |
| Schema change (new column) | Re-run crawler      |
| Initial table creation     | Crawler             |

**Guideline:**

* Use `MSCK REPAIR` for partition updates
* Use crawlers only when schema changes

---

## Operational Considerations

* Crawlers are suitable for initial setup and schema discovery
* Not ideal for frequent production updates
* Prefer controlled schema management with explicit definitions
* Use partition repair for incremental data ingestion

---

## Catalog Output Guarantees

**Provided:**

* Tables are queryable via Athena and ETL
* Partition structure is registered
* Dataset boundaries are correctly defined

**Not Provided:**

* Accurate data types
* Data quality guarantees
* Schema stability in fact tables
* Automatic partition updates