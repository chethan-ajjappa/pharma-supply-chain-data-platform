# S3 Data Lake Architecture

**Phase:** Raw Data Landing  

---

## 1. Zone Strategy

Three-zone data lake architecture with dedicated S3 buckets per layer.

| Zone | Bucket | Format | Purpose |
|---|---|---|---|
| Raw (Bronze) | `pharma-raw-dev` | CSV | Immutable source data |
| Curated (Silver) | `pharma-curated-dev` | Parquet | Cleaned, query-optimized |
| Warehouse (Gold) | `pharma-warehouse-dev` | Parquet | Analytics-ready |
| Query Results | `pharma-athena-results-dev` | — | Athena outputs |

---

## 2. Design Principles

- Raw data is immutable and append-only  
- Format transformation happens downstream  
- Each zone is independently managed  
- Raw acts as system of record  

---

## 3. Raw Zone Folder Layout

```
s3://pharma-raw-dev/
├── dimensions/
│   ├── customers/
│   ├── plant_warehouse/
│   ├── products_master/
│   ├── manufacturers/
│   ├── products_annual_demand/
│   ├── customer_plant_mapping/
│   └── product_manufacturer_bridge/
│
├── facts/
│   ├── orders/
│   │   ├── year=2022/
│   │   │   ├── month=01/
│   │   │   ├── month=02/
│   │   │   └── ...
│   │   ├── year=2023/
│   │   └── ...
│   │
│   ├── product_batches/
│   │   ├── year=2022/
│   │   ├── year=2023/
│   │   └── ...
│   │
│   ├── batch_allocator_central/
│   │   ├── year=2022/
│   │   └── ...
│   │
│   └── inventory_movements/
│       ├── year=2022/
│       └── ...
│
└── _metadata/
    └── manifests/
```

---

## 4. Partitioning Strategy

Fact tables use Hive-style partitioning (`year=YYYY/month=MM`).

| Table | Partitioning |
|---|---|
| orders | year / month |
| product_batches | year |
| batch_allocator_central | year |
| inventory_movements | year |

---

## Key Decisions

- Partitioning applied **before ingestion**  
- Orders split monthly (high volume)  
- Other fact tables partitioned yearly  
- Metadata (manifests) stored alongside raw data for auditability  
