# Phase 1 — Raw Data Profiling: Orders_ERP (2022)

**Generated:** 2026-03-11 23:40:04  
**Source table:** `Orders_ERP_2022.csv`  
**Script:** `profile_orders_erp.py`

---

```
================================================================================
PHASE 1 — RAW DATA PROFILING: Orders_ERP (2022)
================================================================================

STEP 1 — ENTITY INVENTORY
--------------------------------------------------------------------------------
Rows: 760,047
Columns: 42

Column list:
['ERP_EXPORT_ROW_ID', 'ORDER_LINE_ID', 'ORDER_NO', 'SO_NUMBER', 'ORDER_LINE_NO', 'DELIVERY_NO', 'DELIVERY_LINE_NO', 'INVOICE_NO', 'INVOICE_LINE_NO', 'CUSTOMER_ID', 'CUSTOMER_NAME', 'PRODUCT_ID', 'PRODUCT_CODE', 'PRODUCT_NAME', 'PLANT_CODE', 'BATCH_ID', 'BATCH_NO', 'LOT_NUMBER', 'ORDER_DATE', 'EXPECTED_DELIVERY_DATE', 'ACTUAL_DELIVERY_DATE', 'INVOICE_DATE', 'ORDER_QTY', 'DELIVERY_QTY', 'INVOICE_QTY', 'MRP', 'FLOOR_PRICE', 'ORDER_PRICE', 'DELIVERY_PRICE', 'INVOICE_PRICE', 'GST_RATE', 'GST_AMOUNT', 'NET_LINE_AMOUNT', 'TOTAL_LINE_AMOUNT_INCL_GST', 'PAYMENT_MODE', 'PAYMENT_STATUS', 'LINE_STATUS', 'ORDER_STATUS', 'ORDER_YEAR', 'CANCEL_REASON', 'RETURN_REASON', 'RMA_NO']

Data types:
ERP_EXPORT_ROW_ID              object
ORDER_LINE_ID                  object
ORDER_NO                       object
SO_NUMBER                      object
ORDER_LINE_NO                   int64
DELIVERY_NO                    object
DELIVERY_LINE_NO              float64
INVOICE_NO                     object
INVOICE_LINE_NO               float64
CUSTOMER_ID                    object
CUSTOMER_NAME                  object
PRODUCT_ID                      int64
PRODUCT_CODE                   object
PRODUCT_NAME                   object
PLANT_CODE                      int64
BATCH_ID                       object
BATCH_NO                       object
LOT_NUMBER                     object
ORDER_DATE                     object
EXPECTED_DELIVERY_DATE         object
ACTUAL_DELIVERY_DATE           object
INVOICE_DATE                   object
ORDER_QTY                       int64
DELIVERY_QTY                    int64
INVOICE_QTY                     int64
MRP                           float64
FLOOR_PRICE                   float64
ORDER_PRICE                   float64
DELIVERY_PRICE                float64
INVOICE_PRICE                 float64
GST_RATE                      float64
GST_AMOUNT                    float64
NET_LINE_AMOUNT               float64
TOTAL_LINE_AMOUNT_INCL_GST    float64
PAYMENT_MODE                   object
PAYMENT_STATUS                 object
LINE_STATUS                    object
ORDER_STATUS                   object
ORDER_YEAR                      int64
CANCEL_REASON                  object
RETURN_REASON                  object
RMA_NO                         object
dtype: object

Memory usage:
1126.96 MB

STEP 2 — SAMPLE ROWS
--------------------------------------------------------------------------------

First 5 rows:
   ERP_EXPORT_ROW_ID ORDER_LINE_ID         ORDER_NO   SO_NUMBER  ORDER_LINE_NO  ... ORDER_STATUS  ORDER_YEAR CANCEL_REASON  RETURN_REASON RMA_NO
0  ERP20220000000090    OL00000060  ORD-2022-000060  SO00000060             10  ...  Unfulfilled        2022           NaN            NaN    NaN
1  ERP20220000000231    OL00000140  ORD-2022-000140  SO00000140             10  ...  Unfulfilled        2022           NaN            NaN    NaN
2  ERP20220000000304    OL00000177  ORD-2022-000177  SO00000177             10  ...  Unfulfilled        2022           NaN            NaN    NaN
3  ERP20220000000627    OL00000364  ORD-2022-000364  SO00000364             10  ...  Unfulfilled        2022           NaN            NaN    NaN
4  ERP20220000000699    OL00000405  ORD-2022-000405  SO00000405             10  ...  Unfulfilled        2022           NaN            NaN    NaN

[5 rows x 42 columns]

Last 5 rows:
        ERP_EXPORT_ROW_ID ORDER_LINE_ID         ORDER_NO   SO_NUMBER  ORDER_LINE_NO  ... ORDER_STATUS  ORDER_YEAR CANCEL_REASON  RETURN_REASON RMA_NO
760042  ERP20220000759925    OL00244963  ORD-2022-244963  SO00244963             10  ...    Completed        2022           NaN            NaN    NaN
760043  ERP20220000759926    OL00244963  ORD-2022-244963  SO00244963             10  ...    Completed        2022           NaN            NaN    NaN
760044  ERP20220000759952    OL00244971  ORD-2022-244971  SO00244971             10  ...    Completed        2022           NaN            NaN    NaN
760045  ERP20220000759953    OL00244971  ORD-2022-244971  SO00244971             10  ...    Completed        2022           NaN            NaN    NaN
760046  ERP20220000759954    OL00244971  ORD-2022-244971  SO00244971             10  ...    Completed        2022           NaN            NaN    NaN

[5 rows x 42 columns]

Random sample (5 rows):
        ERP_EXPORT_ROW_ID ORDER_LINE_ID         ORDER_NO   SO_NUMBER  ORDER_LINE_NO  ... ORDER_STATUS  ORDER_YEAR CANCEL_REASON  RETURN_REASON RMA_NO
186898  ERP20220000151132    OL00059468  ORD-2022-059468  SO00059468             10  ...    Completed        2022           NaN            NaN    NaN
367090  ERP20220000389664    OL00134466  ORD-2022-134466  SO00134466             10  ...    Completed        2022           NaN            NaN    NaN
162588  ERP20220000201570    OL00075733  ORD-2022-075733  SO00075733             10  ...    Completed        2022           NaN            NaN    NaN
156391  ERP20220000142543    OL00056755  ORD-2022-056755  SO00056755             10  ...    Completed        2022           NaN            NaN    NaN
590378  ERP20220000558340    OL00185611  ORD-2022-185611  SO00185611             10  ...    Completed        2022           NaN            NaN    NaN

[5 rows x 42 columns]

STEP 3 — GRAIN EXPLORATION
--------------------------------------------------------------------------------
Total rows: 760,047
Unique ORDER_LINE_ID: 245,000
Unique ORDER_NO: 245,000
Unique ERP_EXPORT_ROW_ID: 760,047

Unique (ORDER_NO + ORDER_LINE_NO): 245,000

STEP 3A — GRAIN HYPOTHESIS VALIDATION
--------------------------------------------------------------------------------
Checking if ORDER_LINE_ID represents physical grain...
Min rows per ORDER_LINE_ID: 1
Max rows per ORDER_LINE_ID: 25
Avg rows per ORDER_LINE_ID: 3.10
ORDER_LINE_ID is NOT the physical grain

STEP 3B — STATUS MULTIPLICITY ANALYSIS
--------------------------------------------------------------------------------
count    245000.000000
mean          2.546918
std           0.866462
min           1.000000
25%           2.000000
50%           3.000000
75%           3.000000
max           4.000000
Name: LINE_STATUS, dtype: float64

Checking (ORDER_LINE_ID × LINE_STATUS) uniqueness...
Violations found: 109,294
LINE_STATUS does not define unique lifecycle events

STEP 3C — PHYSICAL GRAIN IDENTIFICATION
--------------------------------------------------------------------------------
ERP_EXPORT_ROW_ID unique: True
Observed physical grain:
→ One row per export record
→ Primary Key: ERP_EXPORT_ROW_ID

STEP 4 — PRIMARY KEY DISCOVERY
--------------------------------------------------------------------------------
ERP_EXPORT_ROW_ID         | unique: 760,047 | nulls: 0 | unique_ratio: 1.000
ORDER_LINE_ID             | unique: 245,000 | nulls: 0 | unique_ratio: 0.322
ORDER_NO                  | unique: 245,000 | nulls: 0 | unique_ratio: 0.322
SO_NUMBER                 | unique: 245,000 | nulls: 0 | unique_ratio: 0.322

ORDER_NO + ORDER_LINE_NO  | unique: 245,000 | avg_rows_per_key: 3.10

Key classification:
→ Physical Primary Key      : ERP_EXPORT_ROW_ID (1 row = 1 export event)
→ Logical Order Line ID     : ORDER_LINE_ID (repeats across lifecycle events)
→ Business Identifier       : ORDER_NO + ORDER_LINE_NO (non-unique by design)
→ Snapshot-style PKs        : ❌ Not applicable for this dataset

STEP 5 — RELATIONSHIP DISCOVERY
--------------------------------------------------------------------------------
CUSTOMER_ID overlap with customers: 100.00%
PRODUCT_ID overlap with products_master: 100.00%
PLANT_ID overlap with plant_warehouse: 100.00%
BATCH_ID overlap with product_batches: 100.00% (where not null)

BATCH_ID null count: 254,825 / 760,047
BATCH_ID null percentage: 33.5%

Expected relationships:
Orders_ERP.CUSTOMER_ID → customers.CUSTOMER_ID (M:1)
Orders_ERP.PRODUCT_ID → products_master.product_id (M:1)
Orders_ERP.PLANT_CODE → plant_warehouse.PLANT_ID (M:1)
Orders_ERP.BATCH_ID → product_batches.batch_id (M:1, conditional)

STEP 6 — LIFECYCLE STAGE ANALYSIS
--------------------------------------------------------------------------------
LINE_STATUS distribution:
LINE_STATUS
Delivered    248900
Invoiced     248900
Ordered      245000
Cancelled      9825
Returned       7422
Name: count, dtype: int64

ORDER_STATUS distribution:
ORDER_STATUS
Completed      607478
Partial         81231
Unfulfilled     51688
Cancelled        9825
-                9825
Name: count, dtype: int64

Lifecycle stage indicators:
Rows with ORDER_QTY > 0: 757,237
Rows with DELIVERY_QTY > 0: 497,800
Rows with INVOICE_QTY > 0: 248,900

Document number population:
DELIVERY_NO populated: 505,222
INVOICE_NO populated: 256,322

Date field population:
ORDER_DATE null count: 0
ACTUAL_DELIVERY_DATE null count: 254,825
INVOICE_DATE null count: 503,725

STEP 7 — QUANTITY WATERFALL VALIDATION
--------------------------------------------------------------------------------
Quantity statistics:
           ORDER_QTY   DELIVERY_QTY    INVOICE_QTY
count  760047.000000  760047.000000  760047.000000
mean      444.728933     163.392969      81.532375
std       554.562086     279.636149     214.049924
min         0.000000    -436.000000    -436.000000
25%        75.000000       0.000000       0.000000
50%       230.000000      45.000000       0.000000
75%       490.000000     210.000000      45.000000
max      2000.000000    2000.000000    2000.000000

Quantity waterfall violations: 0 / 760,047

Average fulfillment rate (delivered lines): 68.07%
Average invoice rate (invoiced lines): 100.00%

STEP 8 — PRICE VALIDATION
--------------------------------------------------------------------------------
Price statistics:
                 MRP    FLOOR_PRICE   ORDER_PRICE  DELIVERY_PRICE  INVOICE_PRICE
count  760047.000000  760047.000000  7.600470e+05    5.052220e+05   2.563220e+05
mean      630.516556     567.465045  9.457933e+04    4.922774e+04   4.759633e+04
std      4370.583938    3933.525552  3.942536e+05    2.138019e+05   2.107837e+05
min         1.420000       1.280000  0.000000e+00    2.830000e+00  -1.799622e+06
25%        41.000000      36.900000  7.814800e+03    4.488000e+03   3.960062e+03
50%       101.500000      91.350000  2.184000e+04    1.284300e+04   1.210000e+04
75%       249.000000     224.100000  6.205410e+04    3.543604e+04   3.420000e+04
max    117625.000000  105862.500000  1.031461e+07    1.031461e+07   1.031461e+07

Price bound violations (5% tolerance): 0 / 760,047

GST calculation mismatches: 16 / 760,047
Total amount calculation mismatches: 0 / 760,047

STEP 9 — TEMPORAL ANALYSIS
--------------------------------------------------------------------------------
Date ranges:
ORDER_DATE: 2022-01-01 00:00:00 → 2022-12-31 00:00:00
ACTUAL_DELIVERY_DATE: 2022-01-14 00:00:00 → 2023-02-01 00:00:00
INVOICE_DATE: 2022-01-15 00:00:00 → 2023-01-14 00:00:00

Delivery lag statistics (n=505,222):
count    505222.000000
mean          4.412686
std           2.768176
min           1.000000
25%           2.000000
50%           4.000000
75%           6.000000
max          35.000000
Name: delivery_lag_days, dtype: float64
Invalid delivery dates (before order date): 0

Invoice lag statistics (n=256,322):
count    256322.000000
mean          2.094440
std           3.037923
min         -21.000000
25%           1.000000
50%           2.000000
75%           4.000000
max           5.000000
Name: invoice_lag_days, dtype: float64
Invalid invoice dates (before delivery date): 7,422

ORDER_YEAR mismatch with ORDER_DATE: 0 / 760,047

STEP 10 — BATCH LINKAGE ANALYSIS
--------------------------------------------------------------------------------
Batch linkage patterns:
Total rows: 760,047
Rows with BATCH_ID: 505,222
Rows with DELIVERY_QTY > 0: 497,800

Batch-Delivery crosstab:
DELIVERY_QTY > 0     False   True 
BATCH_ID populated                
False               254825       0
True                  7422  497800

Rows matching expected pattern: 752,625 / 760,047
Unexpected: BATCH_ID but no delivery: 7,422
Unexpected: Delivery but no BATCH_ID: 0

STEP 11 — STATUS CONSISTENCY VALIDATION
--------------------------------------------------------------------------------
LINE_STATUS vs DELIVERY_QTY:
             Total_Lines  Lines_with_Delivery
LINE_STATUS                                  
Cancelled           9825                    0
Delivered         248900               248900
Invoiced          248900               248900
Ordered           245000                    0
Returned            7422                    0

LINE_STATUS vs INVOICE_QTY:
             Total_Lines  Lines_with_Invoice
LINE_STATUS                                 
Cancelled           9825                   0
Delivered         248900                   0
Invoiced          248900              248900
Ordered           245000                   0
Returned            7422                   0

Inconsistent: LINE_STATUS='Ordered' but DELIVERY_QTY > 0: 0
Inconsistent: LINE_STATUS='Delivered' but DELIVERY_QTY = 0: 0

STEP 12 — PAYMENT MODE & STATUS ANALYSIS
--------------------------------------------------------------------------------
PAYMENT_MODE distribution:
PAYMENT_MODE
Card      190177
Cash      190068
Credit    190036
UPI       189766
Name: count, dtype: int64

PAYMENT_STATUS distribution:
PAYMENT_STATUS
Pending    340035
Paid       171112
Name: count, dtype: int64

PAYMENT_MODE vs PAYMENT_STATUS crosstab:
PAYMENT_STATUS    Paid  Pending     All
PAYMENT_MODE                           
Card             42697    85325  128022
Cash             42738    85056  127794
Credit           42896    84902  127798
UPI              42781    84752  127533
All             171112   340035  511147

STEP 13 — CANCELLATION & RETURN ANALYSIS
--------------------------------------------------------------------------------
Cancelled lines: 9,825
Returned lines: 7,422

CANCEL_REASON populated: 9,825
RETURN_REASON populated: 7,422
RMA_NO populated: 7,422

Top cancellation reasons:
CANCEL_REASON
Stock Unavailable     3304
Customer Requested    3268
Order Error           3253
Name: count, dtype: int64

Top return reasons:
RETURN_REASON
Customer Complaint    2506
Damaged               2498
Excess Stock          2418
Name: count, dtype: int64

STEP 14 — ORDER HEADER AGGREGATION
--------------------------------------------------------------------------------
Order header summary:
          line_count  unique_customers  unique_dates  unique_plants      total_qty  total_amount
count  245000.000000          245000.0      245000.0  245000.000000  245000.000000  2.450000e+05
mean        3.102233               1.0           1.0       1.050131    1379.652616  2.934071e+05
std         1.646881               0.0           0.0       0.224739    2510.086282  1.151221e+06
min         1.000000               1.0           1.0       1.000000       0.000000  0.000000e+00
25%         2.000000               1.0           1.0       1.000000     120.000000  1.680000e+04
50%         3.000000               1.0           1.0       1.000000     510.000000  5.376000e+04
75%         3.000000               1.0           1.0       1.000000    1350.000000  1.860000e+05
max        25.000000               1.0           1.0       3.000000   46500.000000  7.220228e+07

Orders with inconsistent CUSTOMER_ID or ORDER_DATE: 0

Orders spanning multiple plants: 11,928 / 245,000

STEP 15 — COMPLETENESS ASSESSMENT
--------------------------------------------------------------------------------
Null counts for critical columns:
ORDER_LINE_ID            :      0 ( 0.00%)
ORDER_NO                 :      0 ( 0.00%)
ORDER_LINE_NO            :      0 ( 0.00%)
CUSTOMER_ID              :      0 ( 0.00%)
PRODUCT_ID               :      0 ( 0.00%)
PLANT_CODE               :      0 ( 0.00%)
ORDER_DATE               :      0 ( 0.00%)
ORDER_QTY                :      0 ( 0.00%)
ORDER_PRICE              :      0 ( 0.00%)
ORDER_YEAR               :      0 ( 0.00%)

Null counts for conditional columns:
BATCH_ID                 : 254,825 (33.53%)
DELIVERY_NO              : 254,825 (33.53%)
INVOICE_NO               : 503,725 (66.28%)
ACTUAL_DELIVERY_DATE     : 254,825 (33.53%)
INVOICE_DATE             : 503,725 (66.28%)
DELIVERY_QTY             :      0 ( 0.00%)
INVOICE_QTY              :      0 ( 0.00%)
CANCEL_REASON            : 750,222 (98.71%)
RETURN_REASON            : 752,625 (99.02%)
RMA_NO                   : 752,625 (99.02%)

DATA QUALITY ASSESSMENT SNAPSHOT
--------------------------------------------------------------------------------
                                                   0
table                                Orders_ERP_2022
rows                                          760047
physical_grain                     ERP_EXPORT_ROW_ID
physical_grain_confirmed                        True
physical_pk_violations                             0
grain_confirmed                                 True
pk_violations                                      0
business_key_violations                       515047
fk_customer_overlap_pct                        100.0
fk_product_overlap_pct                         100.0
fk_plant_overlap_pct                           100.0
qty_waterfall_violations                           0
price_violations                                   0
gst_mismatches                                    16
date_progression_violations                     7422
unexpected_batch_without_delivery               7422
unexpected_delivery_without_batch                  0
critical_nulls_present                         False

MODELING READINESS VERDICT:
READY FOR DIMENSIONAL MODELING (Accumulating Snapshot Fact)

```
