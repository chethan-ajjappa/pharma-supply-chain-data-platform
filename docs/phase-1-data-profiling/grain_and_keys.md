# Grain & Key Design

**Model Type:** Star Schema  
**Fact Strategy:** Event-Level Modeling  
**Data Coverage:** 2022–2025  

---

## Design Principles

- Explicit grain defined for every table  
- Surrogate primary keys for all dimensions  
- Business keys retained for reconciliation only  
- No stored balances in fact tables  
- Many-to-many relationships resolved via bridges  

---

# Dimension Tables

## customers

**Grain**  
One row per customer.

**Primary Key**  
`customer_id`

**Business Key**  
`customer_code`

**SCD Type**  
Type 1

---

## plant_warehouse

**Grain**  
One row per physical plant.

**Primary Key**  
`plant_id`

**Business Key**  
`plant_code`

**SCD Type**  
Type 1

---

## products_master

**Grain**  
One row per SKU.

**Primary Key**  
`product_id`

**Business Key**  
`product_code` (non-unique; family-level identifier)

**SCD Type**  
Type 1

---

## manufacturers

**Grain**  
One row per manufacturer.

**Primary Key**  
`manufacturer_id`

**Business Key**  
`manufacturer_name`

**SCD Type**  
Type 1

---

## products_annual_demand

**Grain**  
One row per product per year.

**Primary Key**  
(`product_id`, `year`)

---

# Bridge Tables

## customer_plant_mapping

**Grain**  
One row per customer × plant relationship.

**Primary Key**  
(`customer_id`, `plant_id`)

---

## product_manufacturer_bridge

**Grain**  
One row per product × manufacturer relationship.

**Primary Key**  
(`product_id`, `manufacturer_id`)

---

# Fact Tables

## product_batches

**Grain**  
One row per manufacturing batch.

**Primary Key**  
`batch_id`

**Fact Type**  
Accumulating Snapshot

---

## batch_allocator_central

**Grain**  
One row per batch × plant × allocation date.

**Primary Key**  
`allocation_id`

**Fact Type**  
Transactional

---

## inventory_movements

**Grain**  
One row per inventory movement event.

**Primary Key**  
`movement_id`

**Fact Type**  
Transactional (ledger-based)

---

## orders_erp

**Grain**  
One row per ERP export lifecycle event.

**Primary Key**  
`erp_export_row_id`

**Fact Type**  
Lifecycle Event Fact


# Key Modeling Decisions

- Surrogate keys used for all dimension joins.
- Event-level facts prevent reconciliation drift. 
- Inventory balances are derived via aggregation.
- Order lifecycle is preserved by modeling export-level grain. 
- M:N relationships are resolved via bridge tables.