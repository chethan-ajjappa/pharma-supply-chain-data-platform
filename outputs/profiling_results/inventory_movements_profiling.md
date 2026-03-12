# Phase 1 — Raw Data Profiling: inventory_movements

**Generated:** 2026-03-12 00:08:32  
**Source table:** `Inventorymovements.csv`  
**Script:** `profile_inventory_movements.py`

---

```
================================================================================
PHASE 1 — RAW DATA PROFILING: inventory_movements
================================================================================

STEP 1 — ENTITY INVENTORY
--------------------------------------------------------------------------------
Rows: 1,042,914
Columns: 19

Column list:
['movement_id', 'movement_type', 'batch_id', 'batch_number', 'lot_number', 'allocation_id', 'product_id', 'manufacturer_id', 'from_plant_id', 'to_plant_id', 'units', 'movement_date', 'movement_year', 'mfg_date', 'expiry_date', 'demand_tier', 'formulation', 'ColdChainRequired', 'operational_zone']

Data types:
movement_id           object
movement_type         object
batch_id              object
batch_number          object
lot_number            object
allocation_id         object
product_id             int64
manufacturer_id       object
from_plant_id        float64
to_plant_id            int64
units                  int64
movement_date         object
movement_year          int64
mfg_date              object
expiry_date           object
demand_tier           object
formulation           object
ColdChainRequired      int64
operational_zone      object
dtype: object

STEP 2 — SAMPLE ROWS
--------------------------------------------------------------------------------

First 5 rows (selected columns):
| movement_id      | movement_type   | batch_id                             | allocation_id                                   |   product_id |   from_plant_id |   to_plant_id |   units | movement_date   |
|:-----------------|:----------------|:-------------------------------------|:------------------------------------------------|-------------:|----------------:|--------------:|--------:|:----------------|
| MOV-2022-0000001 | IN              | BTC-20220101-100021-MFG0004-81B0CAC6 | ALLOC-BTC-20220101-100021-MFG0004-81B0CAC6-1001 |       100021 |             nan |          1001 |    3987 | 2022-01-06      |
| MOV-2022-0000002 | IN              | BTC-20220101-100021-MFG0004-81B0CAC6 | ALLOC-BTC-20220101-100021-MFG0004-81B0CAC6-1006 |       100021 |             nan |          1006 |    3986 | 2022-01-06      |
| MOV-2022-0000003 | IN              | BTC-20220101-100021-MFG0004-81B0CAC6 | ALLOC-BTC-20220101-100021-MFG0004-81B0CAC6-1007 |       100021 |             nan |          1007 |    3986 | 2022-01-06      |
| MOV-2022-0000004 | IN              | BTC-20220101-100021-MFG0004-81B0CAC6 | ALLOC-BTC-20220101-100021-MFG0004-81B0CAC6-1008 |       100021 |             nan |          1008 |    3986 | 2022-01-06      |
| MOV-2022-0000005 | IN              | BTC-20220101-100069-MFG0003-1065BB65 | ALLOC-BTC-20220101-100069-MFG0003-1065BB65-1001 |       100069 |             nan |          1001 |    4231 | 2022-01-06      |

Random sample (5 rows, selected columns):
| movement_id      | movement_type   | batch_id                             | allocation_id                                   |   product_id |   from_plant_id |   to_plant_id |   units | movement_date   |
|:-----------------|:----------------|:-------------------------------------|:------------------------------------------------|-------------:|----------------:|--------------:|--------:|:----------------|
| MOV-2024-0104017 | TRANSFER        | BTC-20240307-100153-MFG0004-E1582350 | nan                                             |       100153 |            1004 |          1016 |     871 | 2024-03-30      |
| MOV-2025-0228637 | TRANSFER        | BTC-20251002-100562-MFG0003-28142096 | nan                                             |       100562 |            1007 |          1053 |    1317 | 2025-10-19      |
| MOV-2025-0275070 | TRANSFER        | BTC-20251204-100567-MFG0001-F2F79147 | nan                                             |       100567 |            1007 |          1047 |    2073 | 2025-12-22      |
| MOV-2022-0011642 | IN              | BTC-20220706-100520-MFG0005-C12CCCFC | ALLOC-BTC-20220706-100520-MFG0005-C12CCCFC-1005 |       100520 |             nan |          1005 |   12992 | 2022-07-15      |
| MOV-2024-0226153 | TRANSFER        | BTC-20241006-100106-MFG0009-DE321D20 | nan                                             |       100106 |            1003 |          1022 |     116 | 2024-10-27      |

STEP 3 — GRAIN EXPLORATION
--------------------------------------------------------------------------------
Total rows: 1,042,914
Unique movement_id: 1,042,914
Unique batch_id: 23,658
Unique allocation_id: 94,306

Composite grain check (batch_id + allocation_id + movement_date):
Unique composite rows: 252,946

Initial grain hypothesis:
→ One row per inventory movement event

STEP 4 — PRIMARY KEY DISCOVERY
--------------------------------------------------------------------------------
movement_id          | unique: 1,042,914 | nulls: 0
batch_id             | unique: 23,658 | nulls: 0
allocation_id        | unique: 94,306 | nulls: 948,608

Likely PK:
→ movement_id (system-generated surrogate key)

STEP 5 — RELATIONSHIP DISCOVERY
--------------------------------------------------------------------------------
batch_id overlap with product_batches: 100.00%
allocation_id overlap with batch_allocator_central: 9.04%
product_id overlap with products_master: 100.00%
manufacturer_id overlap with manufacturers: 100.00%

Expected relationships:
inventory_movements.batch_id → product_batches.batch_id (M:1)
inventory_movements.allocation_id → batch_allocator_central.allocation_id (M:1)
inventory_movements.product_id → products_master.product_id (M:1)
inventory_movements.manufacturer_id → manufacturers.manufacturer_id (M:1)

STEP 6 — MOVEMENT SEMANTICS
--------------------------------------------------------------------------------
Movement type distribution:
movement_type
TRANSFER            855032
IN                   94306
Central_Movement     93576
Name: count, dtype: int64

Null analysis for location columns:
from_plant_id    94306
to_plant_id          0
dtype: int64

Sample movement_type vs from/to plant presence:
      movement_type  from_null  to_null    rows
0  Central_Movement      False    False   93576
1                IN       True    False   94306
2          TRANSFER      False    False  855032

STEP 7 — CARDINALITY HINTS
--------------------------------------------------------------------------------
Movements per batch (top 10):
batch_id
BTC-20251207-100448-MFG0006-D23631AC    57
BTC-20251207-100447-MFG0010-41FCFC97    57
BTC-20251207-100445-MFG0007-ADB285B4    57
BTC-20251207-100444-MFG0007-AA8BB558    57
BTC-20251207-100440-MFG0003-CFAC9930    57
BTC-20251207-100436-MFG0002-BE9FC754    57
BTC-20220101-100247-MFG0002-BC6F6DE6    57
BTC-20220101-100245-MFG0008-2DB1E225    57
BTC-20220101-100218-MFG0008-DED57BA8    57
BTC-20251207-100124-MFG0006-03E4E260    57
dtype: int64

Movements per allocation (top 10):
allocation_id
ALLOC-BTC-20251207-100698-MFG0005-DF55B848-1008    1
ALLOC-BTC-20220101-100012-MFG0002-03991ED4-1001    1
ALLOC-BTC-20251207-100664-MFG0006-3DDD0AD7-1007    1
ALLOC-BTC-20251207-100664-MFG0006-3DDD0AD7-1006    1
ALLOC-BTC-20251207-100664-MFG0006-3DDD0AD7-1002    1
ALLOC-BTC-20251207-100664-MFG0006-3DDD0AD7-1001    1
ALLOC-BTC-20251207-100658-MFG0009-77182E4A-1008    1
ALLOC-BTC-20251207-100658-MFG0009-77182E4A-1007    1
ALLOC-BTC-20251207-100658-MFG0009-77182E4A-1004    1
ALLOC-BTC-20251207-100658-MFG0009-77182E4A-1001    1
dtype: int64

STEP 8 — TEMPORAL COVERAGE
--------------------------------------------------------------------------------
Movement date range: 2022-01-06 00:00:00 → 2025-12-31 00:00:00

Movement years present:
[np.int32(2022), np.int32(2023), np.int32(2024), np.int32(2025)]

STEP 9 — MEASURE VS ATTRIBUTE IDENTIFICATION
--------------------------------------------------------------------------------
Measures:
['units']

Attributes:
['movement_id', 'movement_type', 'batch_id', 'batch_number', 'lot_number', 'allocation_id', 'product_id', 'manufacturer_id', 'from_plant_id', 'to_plant_id', 'movement_date', 'movement_year', 'mfg_date', 'expiry_date', 'demand_tier', 'formulation', 'ColdChainRequired', 'operational_zone']

STEP 10 — PHASE 1 SUMMARY
--------------------------------------------------------------------------------

Table: inventory_movements
Type: FACT (Transactional Ledger)
Grain: One row per inventory movement event
Primary Key: movement_id
Foreign Keys:
  - batch_id → product_batches
  - allocation_id → batch_allocator_central
  - product_id → products_master
  - manufacturer_id → manufacturers
Measures:
  - units
Notes:
  - Movement sign derived from movement_type
  - Inventory balance NOT stored (derived later)


PHASE 1 PROFILING COMPLETE
================================================================================

```
