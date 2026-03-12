#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════
# PRODUCTS_MASTER PROFILING NOTEBOOK
# ═══════════════════════════════════════════════════════════════════════
# Purpose: Code evidence for grain, PK, relationships, data quality
# Table: products_master.csv
# ═══════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
import sys
import io
from pathlib import Path
from datetime import datetime

# =============================================================================
# OUTPUT CAPTURE SETUP
# =============================================================================

class Tee:
    """Writes to both stdout and an internal buffer simultaneously."""
    def __init__(self):
        self._buffer = io.StringIO()
        self._stdout = sys.stdout

    def write(self, data):
        self._stdout.write(data)
        self._buffer.write(data)

    def flush(self):
        self._stdout.flush()
        self._buffer.flush()

    def getvalue(self):
        return self._buffer.getvalue()

_tee = Tee()
sys.stdout = _tee

# =============================================================================
# LOAD DATA
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / 'raw'

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "profiling_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(DATA_DIR / 'products_master.csv')

print("=" * 80)
print("PRODUCTS MASTER - INITIAL LOAD")
print("=" * 80)
print(f"Rows: {len(df)}")
print(f"Columns: {len(df.columns)}")
print(f"\nColumn list:\n{list(df.columns)}")
print(f"\nData types:\n{df.dtypes}")

# ═══════════════════════════════════════════════════════════════════════
# STEP 1: GRAIN VALIDATION
# Grain: ONE ROW PER PRODUCT SKU
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("STEP 1: GRAIN VALIDATION")
print("=" * 70)

total_rows = len(df)
unique_products = df["product_id"].nunique()

print(f"Total rows: {total_rows}")
print(f"Unique product_id: {unique_products}")

grain_valid = total_rows == unique_products
print(f"{'PASS' if grain_valid else 'FAIL'}: Grain validation (1 row per product)")

# ═══════════════════════════════════════════════════════════════════════
# STEP 2: PRIMARY & BUSINESS KEY VALIDATION
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("STEP 2: PRIMARY & BUSINESS KEY VALIDATION")
print("=" * 70)

pk_unique = df["product_id"].is_unique and df["product_id"].isnull().sum() == 0
bk_unique = df["ProductCode"].is_unique and df["ProductCode"].isnull().sum() == 0

print(f"product_id unique & non-null: {'Yes' if pk_unique else 'No'}")
print(f"ProductCode unique & non-null: {'Yes' if bk_unique else 'No'}")

product_id_unique = df["product_id"].nunique()
print(f"\nPRODUCT_ID unique values: {product_id_unique}")

# Check GENERIC_CODE (should NOT be unique - many products per generic)
generic_code_unique = df["GENERIC_CODE"].nunique()
print(f"\nGENERIC_CODE unique values: {generic_code_unique} (should be < total rows)")
print(f"Products per generic (avg): {total_rows / generic_code_unique:.2f}")

# ═══════════════════════════════════════════════════════════════════════
# STEP 3: DOMAIN VALIDATION
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("STEP 3: DOMAIN VALIDATION")
print("=" * 70)

# 3A: FORMULATION
print("\n3A: FORMULATION Domain")
print("-" * 50)
formulations = df["FORMULATION"].value_counts()
print(f"Unique FORMULATION values: {df['FORMULATION'].nunique()}")
print(formulations)

expected_formulations = {
    'Tablet', 'Injection', 'Oral Suspension', 'Powder for Injection',
    'Oral Drops', 'Dry Syrup', 'Capsule', 'Syrup', 'Cream', 'Ointment'
}
actual_formulations = set(df["FORMULATION"].dropna().unique())
unexpected_formulations = actual_formulations - expected_formulations

if unexpected_formulations:
    print(f"\nUnexpected FORMULATION values: {unexpected_formulations}")
else:
    print(f"\nAll FORMULATION values are expected")

# 3B: PACKING_TYPE
print("\n3B: PACKING_TYPE Domain")
print("-" * 50)
packing_types = df["PACKING_TYPE"].value_counts()
print(f"Unique PACKING_TYPE values: {df['PACKING_TYPE'].nunique()}")
print(packing_types)

expected_packing = {'Strip', 'Vial', 'Bottle', 'Box', 'Tube', 'Sachet'}
actual_packing = set(df["PACKING_TYPE"].dropna().unique())
unexpected_packing = actual_packing - expected_packing

if unexpected_packing:
    print(f"\nUnexpected PACKING_TYPE values: {unexpected_packing}")
else:
    print(f"\nAll PACKING_TYPE values are expected")

# 3C: FORMULATION_CODE
print("\n3C: FORMULATION_CODE Domain")
print("-" * 50)
formulation_codes = df["FORMULATION_CODE"].value_counts()
print(f"Unique FORMULATION_CODE values: {df['FORMULATION_CODE'].nunique()}")
print(formulation_codes.head(10))

# Check FORMULATION_CODE consistency with FORMULATION
formulation_mapping = df.groupby('FORMULATION')['FORMULATION_CODE'].value_counts()
print(f"\nFORMULATION to CODE mapping sample:")
print(formulation_mapping.head(15))

# 3D: Boolean Flags
print("\n3D: Boolean Flags")
print("-" * 50)

print(f"IsEssentialDrug values: {df['IsEssentialDrug'].unique()}")
print(f"  Distribution:\n{df['IsEssentialDrug'].value_counts()}")

print(f"\nColdChainRequired values: {df['ColdChainRequired'].unique()}")
print(f"  Distribution:\n{df['ColdChainRequired'].value_counts()}")

print(f"\nIs_discontinued values: {df['Is_discontinued'].unique()}")
print(f"  Distribution:\n{df['Is_discontinued'].value_counts()}")

# 3E: GST_Rate
print("\n3E: GST_Rate Domain")
print("-" * 50)
gst_rates = df["GST_Rate"].value_counts().sort_index()
print(f"GST_Rate values found:\n{gst_rates}")

expected_gst = {0, 5, 12, 18, 28}
actual_gst = set(df["GST_Rate"].dropna().unique())
unexpected_gst = actual_gst - expected_gst

if unexpected_gst:
    print(f"\nUnexpected GST_Rate values: {unexpected_gst}")
else:
    print(f"\nAll GST_Rate values are valid")

# ═══════════════════════════════════════════════════════════════════════
# STEP 4: COMPOSITION CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("STEP 4: COMPOSITION CONSISTENCY")
print("=" * 70)

print("\n4A: ProductCode Format Analysis")
print("-" * 50)

print("Sample ProductCodes:")
print(df['ProductCode'].head(10).tolist())

df['code_matches_generic'] = df.apply(
    lambda row: str(row['ProductCode']).startswith(str(row['GENERIC_CODE']))
    if pd.notna(row['ProductCode']) and pd.notna(row['GENERIC_CODE'])
    else False,
    axis=1
)
code_match_rate = df['code_matches_generic'].sum() / len(df) * 100

print(f"\nProductCode starts with GENERIC_CODE: {code_match_rate:.1f}% of rows")

if code_match_rate < 80:
    print("Low match rate - ProductCode and GENERIC_CODE may not be aligned")
    mismatches = df[~df['code_matches_generic']][['product_id', 'ProductCode', 'GENERIC_CODE']].head(10)
    print("\nSample mismatches:")
    print(mismatches)

# ═══════════════════════════════════════════════════════════════════════
# STEP 5: BUSINESS RULE VALIDATION
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("STEP 5: BUSINESS RULE VALIDATION")
print("=" * 70)

business_rule_errors = 0

# 5A: Price validation
print("\n5A: Price Validation")
print("-" * 50)

negative_prices = df[df['Price'] < 0]
zero_prices = df[df['Price'] == 0]
null_prices = df['Price'].isnull().sum()

print(f"Negative prices: {len(negative_prices)} rows")
print(f"Zero prices: {len(zero_prices)} rows")
print(f"Null prices: {null_prices} rows")

if len(negative_prices) > 0:
    print(f"FAIL: Negative prices found")
    business_rule_errors += 1
else:
    print(f"PASS: No negative prices")

print(f"\nPrice statistics:")
print(df['Price'].describe())

# 5B: ShelfLife validation
print("\n5B: ShelfLife Validation")
print("-" * 50)

print(f"ShelfLife_Months range: {df['ShelfLife_Months'].min()} to {df['ShelfLife_Months'].max()}")
print(f"ShelfLife_Months nulls: {df['ShelfLife_Months'].isnull().sum()}")

unreasonable_shelf = df[(df['ShelfLife_Months'] < 1) | (df['ShelfLife_Months'] > 120)]
print(f"Unreasonable shelf life (<1 or >120 months): {len(unreasonable_shelf)} rows")

if len(unreasonable_shelf) > 0:
    print("Products with unusual shelf life:")
    print(unreasonable_shelf[['product_id', 'Product_name', 'ShelfLife_Months']].head(10))

# 5C: PACK_COUNT validation
print("\n5C: PACK_COUNT Validation")
print("-" * 50)

zero_or_negative_pack = df[df['PACK_COUNT'] <= 0]
print(f"Zero or negative PACK_COUNT: {len(zero_or_negative_pack)} rows")

if len(zero_or_negative_pack) > 0:
    print(f"FAIL: Invalid PACK_COUNT found")
    business_rule_errors += 1
else:
    print(f"PASS: All PACK_COUNT values are positive")

# 5D: Strength field
print("\n5D: Strength Field Semantic Validation")
print("-" * 50)

null_strength = df['Strength_Value'].isnull().sum()
print(f"Null Strength_Value: {null_strength}")

print("\nSample Strength_Value values:")
print(df['Strength_Value'].dropna().unique()[:10])

strength_val = df['Strength_Value'].astype(str).str.lower().str.strip()
strength_unit = df['Strength_Unit'].astype(str).str.lower().str.strip()

invalid_patterns = {'na', 'n/a', 'none', 'unknown', 'as directed', 'nan'}
invalid_strength = df[strength_val.isin(invalid_patterns)]

print(f"Invalid semantic strength values: {len(invalid_strength)}")

partial_strength = df[
    (df['Strength_Value'].notna() & df['Strength_Unit'].isna()) |
    (df['Strength_Value'].isna() & df['Strength_Unit'].notna())
]

print(f"Partial strength rows (value/unit mismatch): {len(partial_strength)}")

if len(invalid_strength) == 0 and len(partial_strength) == 0:
    print("Strength data is semantically and structurally valid")
else:
    print("Strength data quality issues detected")

if len(invalid_strength) > 0:
    print("\nSample invalid semantic strength values:")
    print(invalid_strength[['product_id', 'Product_name', 'Strength_Value']].head(5))

if len(partial_strength) > 0:
    print("\nSample partial strength rows:")
    print(partial_strength[['product_id', 'Product_name', 'Strength_Value', 'Strength_Unit']].head(5))

# 5E: ColdChain logic
print("\n5E: Cold Chain Requirement Analysis")
print("-" * 50)

cold_chain_by_formulation = df.groupby('FORMULATION')['ColdChainRequired'].value_counts(normalize=True) * 100
print("Cold Chain Required distribution by FORMULATION (%):")
print(cold_chain_by_formulation.head(20))

# ═══════════════════════════════════════════════════════════════════════
# STEP 6: COMPLETENESS CHECK
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("STEP 6: COMPLETENESS CHECK")
print("=" * 70)

critical_columns = [
    'product_id', 'ProductCode', 'Product_name',
    'Generic_Composition', 'FORMULATION', 'Price'
]

print("\nNull rates for critical columns:")
for col in critical_columns:
    null_count = df[col].isnull().sum()
    null_rate = (null_count / len(df)) * 100
    status = "Yes" if null_rate == 0 else "No"
    print(f"{status} {col}: {null_rate:.2f}% ({null_count} rows)")

print("\nNull rates for optional columns:")
optional_columns = [
    'Strength_Value', 'Strength_Unit', 'short_composition2', 'ShelfLife_Months'
]
for col in optional_columns:
    null_count = df[col].isnull().sum()
    null_rate = (null_count / len(df)) * 100
    print(f"  {col}: {null_rate:.2f}% ({null_count} rows)")

# ═══════════════════════════════════════════════════════════════════════
# STEP 7: DUPLICATE DETECTION
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("STEP 7: DUPLICATE DETECTION")
print("=" * 70)

duplicate_definition_cols = [
    'Generic_Composition', 'Strength_Value', 'Strength_Unit',
    'FORMULATION', 'PACKING_TYPE'
]

grouped = df.groupby(duplicate_definition_cols).size().reset_index(name='count')
duplicates = grouped[grouped['count'] > 1]

print(f"\nProduct definitions appearing multiple times: {len(duplicates)}")

if len(duplicates) > 0:
    print("\nTop duplicate product definitions:")
    print(duplicates.sort_values('count', ascending=False).head(10))
    print("\nThese may be legitimate (different brands) or data quality issues")

    if len(duplicates) > 0:
        sample_dup = duplicates.iloc[0]
        print("\nExample duplicate products:")
        mask = True
        for col in duplicate_definition_cols:
            mask &= (df[col] == sample_dup[col])
        print(df[mask][['product_id', 'ProductCode', 'Product_name', 'Price']].head(5))

# ═══════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("FINAL VALIDATION SUMMARY")
print("=" * 70)

critical_nulls = sum([df[col].isnull().sum() for col in critical_columns])

summary = {
    "Grain valid": grain_valid,
    "Primary key valid": pk_unique,
    "Business key valid": bk_unique,
    "Strength completeness & semantics valid": (len(invalid_strength) == 0 and len(partial_strength) == 0),
    "GST rates valid": len(unexpected_gst) == 0,
    "Business rules valid": business_rule_errors == 0,
    "Critical columns complete": critical_nulls == 0,
}

print()
for k, v in summary.items():
    status = "PASS" if v else "FAIL"
    print(f"{status}: {k}")

passed = sum(summary.values())
total = len(summary)
print(f"\n{'='*70}")
print(f"Passed {passed}/{total} checks ({passed/total*100:.1f}%)")
print(f"{'='*70}")

if passed < total:
    print("\nDATA QUALITY ISSUES DETECTED - Review failures above")
else:
    print("\nALL VALIDATIONS PASSED - Ready for ingestion")

# =============================================================================
# SAVE OUTPUT TO MARKDOWN
# =============================================================================

sys.stdout = _tee._stdout  # restore original stdout

_output_md = OUTPUT_DIR / "products_master_profiling.md"

_md_content = f"""# Phase 1 — Raw Data Profiling: products_master

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Source table:** `products_master.csv`  
**Script:** `{Path(__file__).name}`

---

```
{_tee.getvalue()}
```
"""

_output_md.write_text(_md_content, encoding="utf-8")
print(f"\n Profiling output saved → {_output_md.relative_to(PROJECT_ROOT)}")