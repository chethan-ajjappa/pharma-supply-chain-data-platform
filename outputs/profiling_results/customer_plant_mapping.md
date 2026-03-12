 # Phase 1 - Raw data profiling: customer plant mapping

**Generated:** 2026-03-12 07:57:57
**Source table:** `Customer_plant_mapping.csv`
**Script:** `profile_customer_plant_mapping.py`

---

```
================================================================================
CUSTOMER PLANT MAPPING - INITIAL LOAD
================================================================================
Rows: 3384
Columns:5

 Column list:
 ['MAPPING_ID', 'CUSTOMER_ID', 'PLANT_CODE', 'PRIORITY_FLAG', 'ACTIVE_FLAG']

 Data types:
MAPPING_ID       object
CUSTOMER_ID      object
PLANT_CODE       object
PRIORITY_FLAG    object
ACTIVE_FLAG        bool
dtype: object

1. STRUCTURAL VALIDATION
----------------------------------------
Total rows: 3384
Duplicate (CUSTOMER_ID, PLANT_CODE): 0

2. REFERENTIAL INTEGRITY
----------------------------------------
Invalid CUSTOMER_ID refs: 0
Invalid PLANT_CODE refs: 0

3. CATEGORICAL SANITY
----------------------------------------
priority_flag values:
PRIORITY_FLAG
PRIMARY      2500
SECONDARY     751
BACKUP        133
Name: count, dtype: int64

active_flag values:
ACTIVE_FLAG
True    3384
Name: count, dtype: int64

4A. BUSINESS RULE: ONE PRIMARY PER CUSTOMER
----------------------------------------
Customers without PRIMARY plant: 0
Customers with >1 PRIMARY plant: 0

4B. CUSTOMER COVERAGE SUMMARY
----------------------------------------
Total customers mapped: 2500
Customers with PRIMARY plant: 2500
Customers with SECONDARY plant: 751
Customers with BACKUP plant: 133

```

