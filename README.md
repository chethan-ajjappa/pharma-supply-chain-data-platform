# Pharma Supply Chain Data Platform

Event-Level Dimensional Warehouse on AWS | 4M+ Supply Chain Records (2022–2025)

---

## Problem Statement

Pharmaceutical distribution involves multiple plants, customer accounts, and product SKUs operating across several fiscal years.
Operational ERP exports — including orders, inventory movements, product batches, and allocation logs — are generated independently. Without structured integration, cross-functional analysis across fulfillment, inventory reconciliation, and demand tracking becomes difficult.

This platform consolidates four years of historical supply chain data (2022–2025) into an analytics-ready warehouse on AWS, enabling reliable answers to questions such as:

- Which product batches are nearing expiry across specific plants?
- Where does planned batch allocation differ from actual inventory movement?
- Which customers are being fulfilled by secondary or alternate plants?
- How does order fulfillment performance vary by therapeutic segment and formulation?
- Where do distribution network imbalances exist between central and regional plants based on allocation, movement, and fulfillment patterns?

The focus of this project is the data engineering foundation — data modeling, transformation pipelines, and orchestration — required to make these analyses repeatable and scalable.

---

# Data Domain

## Business Context

This platform models the distribution network of a pharmaceutical company operating across 57 distribution plants and serving 2,500+ retail stores / distributor accounts with a catalog of 702 product SKUs.

Order fulfillment is governed by a customer–plant routing framework (primary, secondary, backup). Routing decisions vary based on allocation and availability, meaning fulfillment is not tied to a fixed plant.

Batch traceability connects every inventory movement and order fulfillment event back to the originating manufacturing batch and its associated manufacturer, enabling end-to-end product lineage across the network.

---

## Data Model Overview

The platform follows a dimensional modeling approach with explicitly defined grain across all entities.  

Fact tables are stored at event-level granularity — no pre-aggregations are persisted in the warehouse layer.

---

## Dimensional Layer — Master Entities

| Table | Rows | Grain | Role |
|---|---|---|---|
| `customers` | 2,500 | One row per customer account | Retail store / distributor master (surrogate + business key) |
| `plant_warehouse` | 57 | One row per plant | Distribution network master (central + regional) |
| `products_master` | 702 | One row per SKU | Product catalog (formulation, HSN, GST, classification) |
| `manufacturers` | 10 | One row per manufacturer | Licensed manufacturer registry |
| `products_annual_demand` | 702 | One row per SKU | Baseline annual demand planning assumption |

---

## Bridge / Relationship Tables

| Table | Rows | Grain | Role |
|---|---|---|---|
| `customer_plant_mapping` | 3,384 | One row per (customer × plant) | Defines routing priority (primary / secondary / backup) |
| `product_manufacturer_bridge` | 702 | One row per (product × manufacturer) | Defines which manufacturer produces which product |

---

## Transactional / Fact Layer — Event Data (2022–2025)

| Table | Rows | Grain | Role |
|---|---|---|---|
| `product_batches` | 23,658 | One row per manufacturing batch (lot) | Batch master with manufacture date, expiry date, and produced quantity |
| `batch_allocator_central` | 94,306 | One row per (batch × plant × allocation date) | Planned distribution allocation |
| `inventory_movements` | 1,042,914 | One row per inventory movement event | Physical stock movement between plants |
| `orders_erp` | 3,596,681 | One row per order line event | ERP order transactions with fulfillment outcome |


---


