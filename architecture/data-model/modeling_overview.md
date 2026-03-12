# Dimensional Model Overview

**Platform:** Pharma Supply Chain Data Platform  
**Modeling Approach:** Star Schema (Event-Level Fact Design)  
**Source Data:** 11 Operational CSV Tables (2022–2025)

---

## Analytical Scope

This dimensional model enables:

- End-to-end batch traceability (manufacturer → allocation → movement → order fulfillment)
- Order lifecycle performance analysis (Ordered → Delivered → Invoiced → Returned)
- Inventory flow analysis and plant-level stock derivation from movement events
- Allocation vs execution reconciliation
- Customer-to-plant routing behavior analysis
- SKU-level demand vs actual distribution comparison

---

## Model Structure

### Dimensions (5)

- `customers`
- `plant_warehouse`
- `products_master`
- `manufacturers`
- `products_annual_demand`

### Bridge Tables (2)

- `customer_plant_mapping`
- `product_manufacturer_bridge`

### Fact Tables (4)

- `product_batches`
- `batch_allocator_central`
- `inventory_movements`
- `orders_erp`

---

## Data Scale

- ~4.7M total records modeled  
- 3.6M order lifecycle events  
- 1.0M inventory movement events  
- 94K batch allocation events  
- 23K manufacturing batches  
- 4-year coverage (2022–2025)

---

## Core Design Principles

**Event-level facts**  
All fact tables store atomic business events. No derived balances are persisted. Inventory and fulfillment metrics are computed through aggregation to prevent reconciliation drift.

**Surrogate key enforcement**  
All foreign key relationships use surrogate keys. Business keys are retained for reconciliation but never define grain.

**Bridge table resolution of M:N relationships**  
Customer–plant and product–manufacturer relationships are modeled via bridge tables to preserve dimensional integrity.

**Lifecycle-aware order fact**  
`orders_erp` is modeled at the export-event grain to retain full lifecycle history rather than collapsing to order-line level.

---

## Dimensional Pattern Highlights

- Warehouse-generated conformed date dimension shared across all fact tables  
- Role-playing plant dimension (`from_plant_id`, `to_plant_id`)  
- End-to-end batch traceability chain from production to fulfillment

---

## Dimensional Model Diagram

> See `/docs/architecture/logical-erd.png` — generated from dbdiagram.io after Phase 1 profiling.

Key structural features visible in the ERD:
- `dim_date` conforms across all four fact tables
- `dim_plant` serves dual roles: `from_plant_id` and `to_plant_id` in `fact_inventory_movement` (role-playing dimension)
- `bridge_customer_plant` resolves the customer-to-plant many-to-many
- `bridge_product_manufacturer` resolves the product-to-manufacturer many-to-many
- `fact_batch_production` → `fact_batch_allocation` → `fact_inventory_movement` → `fact_order_line_event` forms the batch traceability chain

---