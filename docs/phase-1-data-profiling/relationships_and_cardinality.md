# Relationships & Cardinality

**Phase:** Phase 1 — Pre-Ingestion Data Profiling  
**Purpose:** Consolidated FK relationship matrix with cardinality validation results and referential integrity coverage for all 11 tables.

All relationships were validated against source CSV data before ingestion. Integrity percentages reflect pre-ingestion source quality — not post-ETL results.

---

## FK Relationship Matrix

| Source Table | Source Column | Target Table | Target Column | Cardinality | RI Coverage | Notes |
|---|---|---|---|---|---|---|
| `customer_plant_mapping` | `customer_id` | `customers` | `customer_id` | M:1 | 100% | All 3,384 mappings resolve |
| `customer_plant_mapping` | `plant_id` | `plant_warehouse` | `plant_id` | M:1 | 100% | All plant codes validated |
| `product_manufacturer_bridge` | `product_id` | `products_master` | `product_id` | M:1 | 100% | All 702 products resolve |
| `product_manufacturer_bridge` | `manufacturer_id` | `manufacturers` | `manufacturer_id` | M:1 | 100% | All 10 manufacturers resolve |
| `products_annual_demand` | `product_id` | `products_master` | `product_id` | 1:1 | 100% | Exact product-level match |
| `product_batches` | `product_id` | `products_master` | `product_id` | M:1 | 100% | 23,658 batches → 702 products |
| `product_batches` | `manufacturer_id` | `manufacturers` | `manufacturer_id` | M:1 | 100% | All batch manufacturers resolve |
| `batch_allocator_central` | `batch_id` | `product_batches` | `batch_id` | M:1 | 100% | 94,306 allocations → batch master |
| `batch_allocator_central` | `product_id` | `products_master` | `product_id` | M:1 | 100% | Validated |
| `batch_allocator_central` | `manufacturer_id` | `manufacturers` | `manufacturer_id` | M:1 | 100% | Validated |
| `batch_allocator_central` | `plant_id` | `plant_warehouse` | `plant_id` | M:1 | 100% | All plant allocations resolve |
| `inventory_movements` | `batch_id` | `product_batches` | `batch_id` | M:1 | 100% | 1,042,914 movements → batch master |
| `inventory_movements` | `product_id` | `products_master` | `product_id` | M:1 | 100% | Validated |
| `inventory_movements` | `manufacturer_id` | `manufacturers` | `manufacturer_id` | M:1 | 100% | Validated |
| `inventory_movements` | `from_plant_id` | `plant_warehouse` | `plant_id` | M:1 | Conditional | NULL for IN movements (correct) |
| `inventory_movements` | `to_plant_id` | `plant_warehouse` | `plant_id` | M:1 | Conditional | NULL for some movement types |
| `inventory_movements` | `allocation_id` | `batch_allocator_central` | `allocation_id` | M:1 | ~30% | Optional — allocation-driven only |
| `orders_erp` | `customer_id` | `customers` | `customer_id` | M:1 | 100% | Mandatory FK — fully populated |
| `orders_erp` | `product_id` | `products_master` | `product_id` | M:1 | 100% | Mandatory FK — fully populated |
| `orders_erp` | `plant_id` | `plant_warehouse` | `plant_id` | M:1 | 100% | Mandatory FK — fully populated |
| `orders_erp` | `batch_id` | `product_batches` | `batch_id` | M:1 | ~33.5% | Conditional — delivery-linked only |

---

## Referential Integrity Summary

**Mandatory FKs (must be 100%):** 10 relationships — **all passed**  
**Conditional FKs (nullable by design):** 3 relationships — **all valid where populated**  
**Total FK relationships validated:** 21  
**Overall RI result:** ✅ Passed — approved for Phase 2 ingestion

---

## Entity Cardinality Map

```
manufacturers (10)
    │
    ├──────────────────────────────────────────────┐
    │                                              │
    ▼                                              ▼
product_batches (23,658)         product_manufacturer_bridge (702)
    │                                              │
    │                                              │
    ├──────────────────┐                           ▼
    │                  │                     products_master (702)
    ▼                  ▼                           │
batch_allocator    inventory_movements             ├─── products_annual_demand (702)
_central (94,306)  (1,042,914)                     │
    │                  │                           ▼
    │                  │              orders_erp (3,596,681)
    ▼                  ▼                           │
plant_warehouse (57) ◄──────────────────────────────┘
    │
    ▼
customer_plant_mapping (3,384)
    │
    ▼
customers (2,500)
```

---

## Cardinality Patterns of Note

### Batch-to-Plant Allocation
- Allocations per batch: max observed 4, typical pattern exactly 4
- Interpretation: zone-based allocation (not ad-hoc) — one Central DC + three Regional DCs per batch
- This is a structural insight into how the distribution network operates

### Orders-to-Lifecycle Events
- Average rows per `order_line_id`: 3.1
- Minimum: 1 (cancelled before delivery)
- Maximum: observed up to 5 (returns with partial re-fulfillment)
- Distribution: Ordered (1×) + Delivered (1×) + Invoiced (1×) = 3 rows per fulfilled line

### Customer-to-Plant Routing
- Primary mappings: 2,500 (exactly one per customer — mandatory)
- Secondary mappings: 751 (30% of customers have a secondary plant)
- Backup mappings: 133 (5.3% of customers have a backup plant)
- Maximum plants per customer: 3 (primary + secondary + backup)

### Product-to-Manufacturer Dependency
- 702 product–manufacturer relationships across 10 manufacturers
- Ratio: 70.2 products per manufacturer on average
- Single-source products (only one manufacturer): **queryable via bridge table**

---

## Role-Playing Dimensions

`dim_plant` functions as a **role-playing dimension** in `fact_inventory_movement`:

| Role | Column | Meaning |
|---|---|---|
| Source plant | `from_plant_id` | Where inventory moved from |
| Destination plant | `to_plant_id` | Where inventory moved to |

Both FK columns reference the same `plant_warehouse` table but represent logically different roles.

`dim_date` is also a **role-playing dimension** in `fact_order_line_event`:

| Role | Column | Meaning |
|---|---|---|
| Order date | `order_date_key` | When the order was placed |
| Delivery date | `delivery_date_key` | When delivered (conditional) |
| Invoice date | `invoice_date_key` | When invoiced (conditional) |

---

## Batch Traceability Chain

The full end-to-end traceability path from manufacturer to order fulfillment:

```
manufacturers
    └── product_manufacturer_bridge
            └── products_master
                    └── product_batches  (manufactured)
                            └── batch_allocator_central  (planned distribution)
                                    └── inventory_movements  (physical movement)
                                            └── orders_erp  (fulfillment event)
```

This chain enables recall-scenario queries: given a `manufacturer_id` and a date range, trace every batch produced → every plant it was allocated to → every inventory movement made → every order it was used to fulfill → every customer impacted.