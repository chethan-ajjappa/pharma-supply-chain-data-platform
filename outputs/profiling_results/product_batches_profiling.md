# Phase 1 — Raw Data Profiling: product_batches

**Generated:** 2026-03-12 00:16:04  
**Source table:** `product_batches.csv`  
**Script:** `profile_product_batches.py`

---

```
================================================================================
PHASE 1 — RAW DATA PROFILING: product_batches
================================================================================

STEP 1 — ENTITY INVENTORY
--------------------------------------------------------------------------------
Rows: 23,658
Columns: 16

Column list:
['batch_id', 'batch_number', 'lot_number', 'product_id', 'product_code', 'manufacturer_id', 'manufacturer_name', 'mfg_date', 'expiry_date', 'mfg_year', 'formulation', 'generic_name', 'pack_units_per_pack', 'batch_size_units', 'unit_mrp', 'ColdChainRequired']

Data types:
batch_id                object
batch_number            object
lot_number              object
product_id               int64
product_code            object
manufacturer_id         object
manufacturer_name       object
mfg_date                object
expiry_date             object
mfg_year                 int64
formulation             object
generic_name            object
pack_units_per_pack      int64
batch_size_units         int64
unit_mrp               float64
ColdChainRequired        int64
dtype: object

STEP 2 — SAMPLE ROWS
--------------------------------------------------------------------------------

First 5 rows (selected columns):
                               batch_id              batch_number  product_id manufacturer_id    mfg_date expiry_date  batch_size_units
0  BTC-20220101-100012-MFG0002-03991ED4  MFG0002/100012/2022/4280      100012         MFG0002  2022-01-01  2023-01-31             48444
1  BTC-20220101-100021-MFG0004-81B0CAC6  MFG0004/100021/2022/9067      100021         MFG0004  2022-01-01  2023-07-31             15945
2  BTC-20220101-100046-MFG0008-B1191041  MFG0008/100046/2022/3907      100046         MFG0008  2022-01-01  2024-01-31             47891
3  BTC-20220101-100054-MFG0004-AC390F0C  MFG0004/100054/2022/8109      100054         MFG0004  2022-01-01  2023-01-31             49804
4  BTC-20220101-100055-MFG0005-2A247FBA  MFG0005/100055/2022/2246      100055         MFG0005  2022-01-01  2023-01-31             47818

Random sample (5 rows, selected columns):
                                   batch_id              batch_number  product_id manufacturer_id    mfg_date expiry_date  batch_size_units
18964  BTC-20250402-100344-MFG0005-B2EAFE0F  MFG0005/100344/2025/7496      100344         MFG0005  2025-04-02  2028-04-30             63228
20011  BTC-20250603-100156-MFG0008-B963F2C8  MFG0008/100156/2025/8983      100156         MFG0008  2025-06-03  2026-06-30               689
2290   BTC-20220506-100147-MFG0001-90A77B95  MFG0001/100147/2022/7849      100147         MFG0001  2022-05-06  2025-05-31             51422
16443  BTC-20241105-100271-MFG0004-123F1B46  MFG0004/100271/2024/6766      100271         MFG0004  2024-11-05  2025-11-30             58764
9966   BTC-20231004-100303-MFG0010-1E60BFA5  MFG0010/100303/2023/6977      100303         MFG0010  2023-10-04  2025-10-31             57577

STEP 3 — GRAIN EXPLORATION
--------------------------------------------------------------------------------
Total rows: 23,658
Unique batch_id: 23,658
Unique batch_number: 23,651
Unique lot_number: 23,658

 Initial grain hypothesis:
-> One row per manufacturing batch (batch_id)

STEP 4 — KEY DISCOVERY
--------------------------------------------------------------------------------
batch_id        | unique: 23,658 | nulls: 0
batch_number    | unique: 23,651 | nulls: 0
lot_number      | unique: 23,658 | nulls: 0

Likely PK:
→ batch_id (system-generated surrogate key)

STEP 5 — RELATIONSHIP DISCOVERY
--------------------------------------------------------------------------------
product_id overlap with products_master: 100.00%
manufacturer_id overlap with manufacturers: 100.00%

Expected relationships:
product_batches.product_id → products_master.product_id (M:1)
product_batches.manufacturer_id → manufacturers.manufacturer_id (M:1)

STEP 6 — CARDINALITY HINTS
--------------------------------------------------------------------------------
Batches per product (top 5):
product_id
100565    48
100564    48
100563    48
100562    48
100561    48
dtype: int64

Batches per manufacturer (top 5):
manufacturer_id
MFG0010    2594
MFG0008    2578
MFG0006    2458
MFG0007    2386
MFG0005    2362
dtype: int64

STEP 7 — TEMPORAL COVERAGE
--------------------------------------------------------------------------------
Manufacturing date range: 2022-01-01 00:00:00 → 2025-12-07 00:00:00
Expiry date range: 2022-07-31 00:00:00 → 2028-12-31 00:00:00

Manufacturing years present:
[np.int32(2022), np.int32(2023), np.int32(2024), np.int32(2025)]

STEP 8 — MEASURE VS ATTRIBUTE IDENTIFICATION
--------------------------------------------------------------------------------
Measures:
['batch_size_units', 'unit_mrp']

Attributes:
['batch_id', 'batch_number', 'lot_number', 'product_id', 'product_code', 'manufacturer_id', 'manufacturer_name', 'mfg_date', 'expiry_date', 'mfg_year', 'formulation', 'generic_name', 'pack_units_per_pack', 'ColdChainRequired']

STEP 9 — PHASE 1 SUMMARY
--------------------------------------------------------------------------------

Table: product_batches
Type: FACT (Accumulating Snapshot)
Grain: One row per manufacturing batch
Primary Key: batch_id
Foreign Keys:
  - product_id → products_master
  - manufacturer_id → manufacturers
Measures:
  - batch_size_units
  - unit_mrp


PHASE 1 PROFILING COMPLETE
================================================================================

```
