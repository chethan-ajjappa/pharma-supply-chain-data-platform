# Phase 1 — Raw Data Profiling: products_master

**Generated:** 2026-03-12 00:17:26  
**Source table:** `products_master.csv`  
**Script:** `profile_products_master.py`

---

```
================================================================================
PRODUCTS MASTER - INITIAL LOAD
================================================================================
Rows: 702
Columns: 22

Column list:
['product_id', 'ProductCode', 'HSNCode', 'Generic_Composition', 'Strength_Value', 'Strength_Unit', 'FORMULATION', 'PACKING_TYPE', 'pack_size_label', 'Therapeutic Segment', 'IsEssentialDrug', 'ColdChainRequired', 'GST_Rate', 'Product_name', 'FORMULATION_CODE', 'PACK_COUNT', 'GENERIC_CODE', 'Is_discontinued', 'Price', 'short_composition1', 'short_composition2', 'ShelfLife_Months']

Data types:
product_id               int64
ProductCode             object
HSNCode                  int64
Generic_Composition     object
Strength_Value          object
Strength_Unit           object
FORMULATION             object
PACKING_TYPE            object
pack_size_label         object
Therapeutic Segment     object
IsEssentialDrug          int64
ColdChainRequired        int64
GST_Rate                 int64
Product_name            object
FORMULATION_CODE        object
PACK_COUNT               int64
GENERIC_CODE            object
Is_discontinued           bool
Price                  float64
short_composition1      object
short_composition2      object
ShelfLife_Months         int64
dtype: object

======================================================================
STEP 1: GRAIN VALIDATION
======================================================================
Total rows: 702
Unique product_id: 702
PASS: Grain validation (1 row per product)

======================================================================
STEP 2: PRIMARY & BUSINESS KEY VALIDATION
======================================================================
product_id unique & non-null: Yes
ProductCode unique & non-null: No

PRODUCT_ID unique values: 702

GENERIC_CODE unique values: 560 (should be < total rows)
Products per generic (avg): 1.25

======================================================================
STEP 3: DOMAIN VALIDATION
======================================================================

3A: FORMULATION Domain
--------------------------------------------------
Unique FORMULATION values: 26
FORMULATION
Tablet                    314
Injection                 145
Bottle                     99
Packet                     30
Tube                       27
Nasal Spray                16
Oral Suspension            14
Powder for Injection       10
Injection (Suspension)      7
Dry Syrup                   5
Soft Gelatin Capsule        5
Ointment                    4
Suppository                 4
Eye/Ear Drops               3
Infusion                    3
Oral Drops                  2
Nasal Solution              2
Dusting Powder              2
Prefilled Syringe           2
Nasal Drops                 2
Vaginal Gel                 1
Tablet (ER)                 1
Pump Bottle                 1
Tablet (SR)                 1
Canister                    1
Box                         1
Name: count, dtype: int64

Unexpected FORMULATION values: {'Nasal Spray', 'Eye/Ear Drops', 'Bottle', 'Infusion', 'Dusting Powder', 'Nasal Solution', 'Tube', 'Packet', 'Prefilled Syringe', 'Pump Bottle', 'Nasal Drops', 'Injection (Suspension)', 'Tablet (SR)', 'Vaginal Gel', 'Box', 'Soft Gelatin Capsule', 'Tablet (ER)', 'Suppository', 'Canister'}

3B: PACKING_TYPE Domain
--------------------------------------------------
Unique PACKING_TYPE values: 10
PACKING_TYPE
Strip        323
Vial         143
Bottle       138
Packet        47
Tube          32
Pen           10
Ampoule        4
Box            2
Prefilled      2
Canister       1
Name: count, dtype: int64

Unexpected PACKING_TYPE values: {'Prefilled', 'Ampoule', 'Packet', 'Canister', 'Pen'}

3C: FORMULATION_CODE Domain
--------------------------------------------------
Unique FORMULATION_CODE values: 24
FORMULATION_CODE
TAB    314
INJ    152
BTL     99
PKT     30
TUB     27
NSP     16
SUS     14
PFI     10
DRS      5
SGC      5
Name: count, dtype: int64

FORMULATION to CODE mapping sample:
FORMULATION             FORMULATION_CODE
Bottle                  BTL                  99
Box                     BOX                   1
Canister                CST                   1
Dry Syrup               DRS                   5
Dusting Powder          DWP                   2
Eye/Ear Drops           EYEEAR                3
Infusion                INF                   3
Injection               INJ                 145
Injection (Suspension)  INJ                   7
Nasal Drops             OTH                   2
Nasal Solution          OTH                   2
Nasal Spray             NSP                  16
Ointment                ONT                   4
Oral Drops              ODR                   2
Oral Suspension         SUS                  14
Name: count, dtype: int64

3D: Boolean Flags
--------------------------------------------------
IsEssentialDrug values: [1 0]
  Distribution:
IsEssentialDrug
0    497
1    205
Name: count, dtype: int64

ColdChainRequired values: [0 1]
  Distribution:
ColdChainRequired
0    557
1    145
Name: count, dtype: int64

Is_discontinued values: [ True False]
  Distribution:
Is_discontinued
False    637
True      65
Name: count, dtype: int64

3E: GST_Rate Domain
--------------------------------------------------
GST_Rate values found:
GST_Rate
5      43
12    659
Name: count, dtype: int64

All GST_Rate values are valid

======================================================================
STEP 4: COMPOSITION CONSISTENCY
======================================================================

4A: ProductCode Format Analysis
--------------------------------------------------
Sample ProductCodes:
['CEFIXI-300MG-TAB-STRIP', 'AZITHR-250MG-TAB-STRIP', 'AZITHR-250MG-TAB-STRIP', 'AZITHR-500MG-INJ-VIAL', 'CEFURO-125MG-TAB-STRIP', 'AMOXYC-500MG-TAB-STRIP', 'AMOXYC-200MG-SUS-BOTTLE', 'AMOXYC-400mg/5ml-SUS-BOTTLE', 'AMOXYC-875MG-TAB-STRIP', 'AMOXYC-250MG-TAB-STRIP']

ProductCode starts with GENERIC_CODE: 100.0% of rows

======================================================================
STEP 5: BUSINESS RULE VALIDATION
======================================================================

5A: Price Validation
--------------------------------------------------
Negative prices: 0 rows
Zero prices: 0 rows
Null prices: 0 rows
PASS: No negative prices

Price statistics:
count       702.000000
mean       1184.288376
std        7179.619428
min           1.420000
25%          48.225000
50%         119.700000
75%         330.565000
max      117625.000000
Name: Price, dtype: float64

5B: ShelfLife Validation
--------------------------------------------------
ShelfLife_Months range: 6 to 36
ShelfLife_Months nulls: 0
Unreasonable shelf life (<1 or >120 months): 0 rows

5C: PACK_COUNT Validation
--------------------------------------------------
Zero or negative PACK_COUNT: 9 rows
FAIL: Invalid PACK_COUNT found

5D: Strength Field Semantic Validation
--------------------------------------------------
Null Strength_Value: 8

Sample Strength_Value values:
['300' '250' '500' '125' '200' '400mg/5ml' '875' '1000' '80' '125mg/5ml']
Invalid semantic strength values: 8
Partial strength rows (value/unit mismatch): 151
Strength data quality issues detected

Sample invalid semantic strength values:
     product_id                  Product_name Strength_Value
26       100027       Flemoxin CV Plus Tablet            NaN
532      100533             Dolofin Injection            NaN
551      100552  Diclofenac Sodium  Injection            NaN
679      100680                   Opv Vaccine            NaN
680      100681            Oral Polio Vaccine            NaN

Sample partial strength rows:
    product_id                 Product_name Strength_Value Strength_Unit
7       100008     Augmentin DDS Suspension      400mg/5ml           NaN
17      100018                Axn Dry Syrup      125mg/5ml           NaN
21      100022              Avmox Dry Syrup      125mg/5ml           NaN
32      100033  Aricef O 50mg/5ml Dry Syrup       50mg/5ml           NaN
33      100034          Avoxime O Dry Syrup       50mg/5ml           NaN

5E: Cold Chain Requirement Analysis
--------------------------------------------------
Cold Chain Required distribution by FORMULATION (%):
FORMULATION             ColdChainRequired
Bottle                  0                    100.0
Box                     0                    100.0
Canister                0                    100.0
Dry Syrup               0                    100.0
Dusting Powder          0                    100.0
Eye/Ear Drops           0                    100.0
Infusion                0                    100.0
Injection               1                    100.0
Injection (Suspension)  0                    100.0
Nasal Drops             0                    100.0
Nasal Solution          0                    100.0
Nasal Spray             0                    100.0
Ointment                0                    100.0
Oral Drops              0                    100.0
Oral Suspension         0                    100.0
Packet                  0                    100.0
Powder for Injection    0                    100.0
Prefilled Syringe       0                    100.0
Pump Bottle             0                    100.0
Soft Gelatin Capsule    0                    100.0
Name: proportion, dtype: float64

======================================================================
STEP 6: COMPLETENESS CHECK
======================================================================

Null rates for critical columns:
Yes product_id: 0.00% (0 rows)
Yes ProductCode: 0.00% (0 rows)
Yes Product_name: 0.00% (0 rows)
Yes Generic_Composition: 0.00% (0 rows)
Yes FORMULATION: 0.00% (0 rows)
Yes Price: 0.00% (0 rows)

Null rates for optional columns:
  Strength_Value: 1.14% (8 rows)
  Strength_Unit: 22.65% (159 rows)
  short_composition2: 72.65% (510 rows)
  ShelfLife_Months: 0.00% (0 rows)

======================================================================
STEP 7: DUPLICATE DETECTION
======================================================================

Product definitions appearing multiple times: 81

Top duplicate product definitions:
    Generic_Composition Strength_Value Strength_Unit FORMULATION PACKING_TYPE  count
204           Ibuprofen            400            mg      Tablet        Strip      4
304          Omeprazole             20            mg      Tablet        Strip      4
67           Cefuroxime            1.5            gm   Injection         Vial      3
40         Azithromycin            250            mg      Tablet        Strip      3
105       Dapagliflozin             10            mg      Tablet        Strip      3
406      Spironolactone             25            mg      Tablet        Strip      3
404          Sofosbuvir            400            mg      Tablet        Strip      3
100         Clopidogrel             75            mg      Tablet        Strip      3
405      Spironolactone            100            mg      Tablet        Strip      3
134         Domperidone             30            mg      Tablet        Strip      3

These may be legitimate (different brands) or data quality issues

Example duplicate products:
     product_id           ProductCode       Product_name  Price
113      100114  AMLODI-5MG-TAB-STRIP  Amlokind 5 Tablet  22.15
114      100115  AMLODI-5MG-TAB-STRIP     Amlip 5 Tablet  32.36
115      100116  AMLODI-5MG-TAB-STRIP   Amlodac 5 Tablet  97.10

======================================================================
FINAL VALIDATION SUMMARY
======================================================================

PASS: Grain valid
PASS: Primary key valid
FAIL: Business key valid
FAIL: Strength completeness & semantics valid
PASS: GST rates valid
FAIL: Business rules valid
PASS: Critical columns complete

======================================================================
Passed 4/7 checks (57.1%)
======================================================================

DATA QUALITY ISSUES DETECTED - Review failures above

```
