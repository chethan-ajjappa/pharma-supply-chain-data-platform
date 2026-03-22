# Validation Results

Generated: 2026-03-23 00:33:21.122561

## Overall Status
READY FOR PHASE 2

## Detailed Results

| Category | Test | Status | Violations | Notes |
|----------|------|--------|------------|-------|
| FK Integrity | product_id in batches | PASS | 0 |  |
| FK Integrity | manufacturer_id in batches | PASS | 0 |  |
| FK Integrity | plant_id in allocations | PASS | 0 |  |
| FK Integrity | from_plant_id in movements | PASS | 0 |  |
| FK Integrity | to_plant_id in movements | PASS | 0 |  |
| FK Integrity | CUSTOMER_ID in orders | PASS | 0 |  |
| FK Integrity | PRODUCT_ID in orders | PASS | 0 |  |
| FK Integrity | PLANT_CODE in orders (mapped to PLANT_ID) | PASS | 0 | (Critical if >0) |
| FK Integrity | CUSTOMER_ID in bridge | PASS | 0 |  |
| FK Integrity | PLANT_CODE in bridge | PASS | 0 |  |
| Business Logic | Quantity waterfall (ORDER ≥ DELIVERY ≥ INVOICE) | PASS | 0 | (Returns may have negatives) |
| Business Logic | Date progression (ORDER ≤ DELIVERY ≤ INVOICE) | PASS | 0 | (Returns may reverse dates) |
| Business Logic | Batch expiry > manufacturing | PASS | 0 |  |
| Business Logic | Movement type semantics | PASS | 0 |  |
| Cross-Year | batch_id global uniqueness (2022-2025) | PASS | 0 |  |
| Cross-Year | ERP_EXPORT_ROW_ID global uniqueness | PASS | 0 |  |
| Cross-Year | ORDER_YEAR matches ORDER_DATE | PASS | 0 |  |
| Grain | batch_id unique (batch_production) | PASS | 0 |  |
| Grain | allocation_id unique (batch_allocation) | PASS | 0 |  |
| Grain | movement_id unique (inventory_movement) | PASS | 0 |  |
| Grain | ERP_EXPORT_ROW_ID unique (order_event) | PASS | 0 |  |
| Nulls | Critical columns in batches | PASS | 0 |  |
| Nulls | Critical columns in orders | PASS | 0 |  |


 ## Results by Category

| category       |   PASS |   total |
|:---------------|-------:|--------:|
| Business Logic |      4 |       4 |
| Cross-Year     |      3 |       3 |
| FK Integrity   |     10 |      10 |
| Grain          |      4 |       4 |
| Nulls          |      2 |       2 |