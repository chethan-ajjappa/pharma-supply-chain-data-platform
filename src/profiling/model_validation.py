#!/usr/bin/env python3
"""
MODEL VALIDATION SCRIPT
═══════════════════════════════════════════════════════════════════════
Purpose: 
    Validate star schema design against CSV data files
    Run this BEFORE uploading to S3

Key Design Principles:
    • Referential Integrity validation
    • Business rule enforcement
    • Grain validation
    • Cross-year consistency
    • Null analysis

Important ERP Note:
    Orders table contains column named `PLANT_CODE`.
    HOWEVER — due to ERP export mislabeling — this column actually stores
    surrogate PLANT_ID values (integers). Therefore FK validation compares
    against plants['PLANT_ID'] intentionally.

Output:
    outputs/profiling_results/validation_results.md

═══════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DATA_DIR = PROJECT_ROOT /  "raw"
ORDER_DIR = DATA_DIR / "orders_erp"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "profiling_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# Results storage
results = []

def log_test(category, test_name, status, count, notes=""):
    """Log validation test result"""
    count = int(count)
    results.append({
        "category": category,
        "test": test_name,
        "status": status,
        "violations": count,
        "notes": notes
    })

    print(
        f"[{status:<5}] | "
        f"{category:<12} | "
        f"{test_name:<45} | "
        f"violations={count:<6} "
        f"{notes}"
    )

print("=" * 80)
print("Validation Results")
print("=" * 80)
print(f"Started: {datetime.now()}\n")

# ═══════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════

print("Loading data files...")

try:
    # Dimensions
    customers = pd.read_csv(DATA_DIR / 'customers.csv')
    products = pd.read_csv(DATA_DIR / 'products_master.csv')
    manufacturers = pd.read_csv(DATA_DIR / 'manufacturers.csv')
    plants = pd.read_csv(DATA_DIR / 'plant_warehouse.csv')

    # Facts (2022-2025)
    batches = pd.read_csv(DATA_DIR / 'product_batches.csv')
    allocations = pd.read_csv(DATA_DIR / 'batchallocatorcentral.csv')
    movements = pd.read_csv(DATA_DIR / 'InventoryMovements.csv')

    # Orders (combine all years)
    orders_2022 = pd.read_csv(ORDER_DIR / 'Orders_ERP_2022.csv')
    orders_2023 = pd.read_csv(ORDER_DIR/ 'Orders_ERP_2023.csv')
    orders_2024 = pd.read_csv(ORDER_DIR / 'Orders_ERP_2024.csv')
    orders_2025 = pd.read_csv(ORDER_DIR / 'Orders_ERP_2025.csv')
    orders = pd.concat(
        [orders_2022, orders_2023, orders_2024, orders_2025], ignore_index=True
        )

    # Bridges
    bridge_cust_plant = pd.read_csv(DATA_DIR / 'customer_plant_mapping.csv')
    bridge_prod_mfr = pd.read_csv(DATA_DIR / 'product_manufacturer_bridge.csv')
    
except Exception as e:
    print(f" Data loading failed: {e}")
    sys.exit(1)

print(f"All source tables loaded successfully\n")

# ═══════════════════════════════════════════════════════════════════════
# 8A. REFERENTIAL INTEGRITY VALIDATION
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("8A. REFERENTIAL INTEGRITY VALIDATION")
print("=" * 80)

# Test 1: batches → products
orphans = batches[~batches['product_id'].isin(products['product_id'])]
log_test('FK Integrity', 'product_id in batches', 
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 2: batches → manufacturers
orphans = batches[~batches['manufacturer_id'].isin(manufacturers['manufacturer_id'])]
log_test('FK Integrity', 'manufacturer_id in batches',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 3: allocations → plants
orphans = allocations[~allocations['plant_id'].isin(plants['PLANT_ID'])]
log_test('FK Integrity', 'plant_id in allocations',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 4: movements → plants (from_plant_id)
valid_from = movements[movements['from_plant_id'].notna()]
orphans = valid_from[~valid_from['from_plant_id'].isin(plants['PLANT_ID'])]
log_test('FK Integrity', 'from_plant_id in movements',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 5: movements → plants (to_plant_id)
valid_to = movements[movements['to_plant_id'].notna()]
orphans = valid_to[~valid_to['to_plant_id'].isin(plants['PLANT_ID'])]
log_test('FK Integrity', 'to_plant_id in movements',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 6: orders → customers
orphans = orders[~orders['CUSTOMER_ID'].isin(customers['CUSTOMER_ID'])]
log_test('FK Integrity', 'CUSTOMER_ID in orders',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 7: orders → products
orphans = orders[~orders['PRODUCT_ID'].isin(products['product_id'])]
log_test('FK Integrity', 'PRODUCT_ID in orders',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 8: orders → plants (PLANT_CODE mapped to PLANT_ID)  ERP mislabeling note: column name says PLANT_CODE but stores surrogate ID
orphans = orders[~orders['PLANT_CODE'].isin(plants['PLANT_ID'])]
log_test('FK Integrity', 'PLANT_CODE in orders (mapped to PLANT_ID)',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans),
         notes="(Critical if >0)")

# Test 9: bridge_customer_plant → customers
orphans = bridge_cust_plant[~bridge_cust_plant['CUSTOMER_ID'].isin(customers['CUSTOMER_ID'])]
log_test('FK Integrity', 'CUSTOMER_ID in bridge',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# Test 10: bridge_customer_plant → plants
orphans = bridge_cust_plant[~bridge_cust_plant['PLANT_CODE'].isin(plants['PLANT_CODE'])]
log_test('FK Integrity', 'PLANT_CODE in bridge',
         'PASS' if len(orphans) == 0 else 'FAIL', len(orphans))

# ═══════════════════════════════════════════════════════════════════════
# 8B. BUSINESS LOGIC
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("8B. BUSINESS LOGIC")
print("=" * 80)

# Test 11: Quantity waterfall
violations = orders[
    (orders['ORDER_QTY'] < orders['DELIVERY_QTY']) |
    (orders['DELIVERY_QTY'] < orders['INVOICE_QTY'])
]
log_test('Business Logic', 'Quantity waterfall (ORDER ≥ DELIVERY ≥ INVOICE)',
         'PASS' if len(violations) == 0 else 'WARN', len(violations),
         notes="(Returns may have negatives)")

# Test 12: Date progression
orders['ORDER_DATE'] = pd.to_datetime(orders['ORDER_DATE'], errors='coerce')
orders['ACTUAL_DELIVERY_DATE'] = pd.to_datetime(orders['ACTUAL_DELIVERY_DATE'], errors='coerce')
orders['INVOICE_DATE'] = pd.to_datetime(orders['INVOICE_DATE'], errors='coerce')

violations = orders[
    (orders['ACTUAL_DELIVERY_DATE'] < orders['ORDER_DATE']) |
    (orders['INVOICE_DATE'] < orders['ACTUAL_DELIVERY_DATE'])
].dropna()

log_test('Business Logic', 'Date progression (ORDER ≤ DELIVERY ≤ INVOICE)',
         'PASS' if len(violations) == 0 else 'WARN', len(violations),
         notes="(Returns may reverse dates)")

# Test 13: Batch expiry logic
batches['mfg_date'] = pd.to_datetime(batches['mfg_date'], errors='coerce')
batches['expiry_date'] = pd.to_datetime(batches['expiry_date'], errors='coerce')

violations = batches[batches['expiry_date'] <= batches['mfg_date']]
log_test('Business Logic', 'Batch expiry > manufacturing',
         'PASS' if len(violations) == 0 else 'FAIL', len(violations))

# Test 14: Movement type semantics
violations = movements[
    ((movements['movement_type'] == 'IN') & movements['from_plant_id'].notna()) |
    ((movements['movement_type'] == 'TRANSFER') & 
     (movements['from_plant_id'].isna() | movements['to_plant_id'].isna())) |
    ((movements['movement_type'] == 'Central_Movement') & 
     (movements['from_plant_id'].isna() | movements['to_plant_id'].isna()))
]
log_test('Business Logic', 'Movement type semantics',
         'PASS' if len(violations) == 0 else 'FAIL', len(violations))

# ═══════════════════════════════════════════════════════════════════════
# 8C. CROSS-YEAR CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("8C. CROSS-YEAR CONSISTENCY CHECKS")
print("=" * 80)

# Test 15: batch_id global uniqueness
duplicates = batches[batches.duplicated(subset='batch_id', keep=False)]
log_test('Cross-Year', 'batch_id global uniqueness (2022-2025)',
         'PASS' if len(duplicates) == 0 else 'FAIL', len(duplicates))

# Test 16: ERP_EXPORT_ROW_ID global uniqueness
duplicates = orders[orders.duplicated(subset='ERP_EXPORT_ROW_ID', keep=False)]
log_test('Cross-Year', 'ERP_EXPORT_ROW_ID global uniqueness',
         'PASS' if len(duplicates) == 0 else 'FAIL', len(duplicates))

# Test 17: ORDER_YEAR alignment
orders['year_extracted'] = orders['ORDER_DATE'].dt.year
mismatches = orders[orders['ORDER_YEAR'] != orders['year_extracted']]
log_test('Cross-Year', 'ORDER_YEAR matches ORDER_DATE',
         'PASS' if len(mismatches) == 0 else 'FAIL', len(mismatches))

# Test 18: Date ranges
print(f"\nℹ️  Date range checks:")
print(f"  batches: {batches['mfg_date'].min()} to {batches['mfg_date'].max()}")
print(f"  orders: {orders['ORDER_DATE'].min()} to {orders['ORDER_DATE'].max()}")

# ═══════════════════════════════════════════════════════════════════════
# 8D. GRAIN VALIDATION
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("8D. GRAIN VALIDATION")
print("=" * 80)

# Test 19: fact_batch_production grain
duplicates = batches[batches.duplicated(subset='batch_id', keep=False)]
log_test('Grain', 'batch_id unique (batch_production)',
         'PASS' if len(duplicates) == 0 else 'FAIL', len(duplicates))

# Test 20: fact_batch_allocation grain
duplicates = allocations[allocations.duplicated(subset='allocation_id', keep=False)]
log_test('Grain', 'allocation_id unique (batch_allocation)',
         'PASS' if len(duplicates) == 0 else 'FAIL', len(duplicates))

# Test 21: fact_inventory_movement grain
duplicates = movements[movements.duplicated(subset='movement_id', keep=False)]
log_test('Grain', 'movement_id unique (inventory_movement)',
         'PASS' if len(duplicates) == 0 else 'FAIL', len(duplicates))

# Test 22: fact_order_line_event grain
duplicates = orders[orders.duplicated(subset='ERP_EXPORT_ROW_ID', keep=False)]
log_test('Grain', 'ERP_EXPORT_ROW_ID unique (order_event)',
         'PASS' if len(duplicates) == 0 else 'FAIL', len(duplicates))

# ═══════════════════════════════════════════════════════════════════════
# 8E. NULL ANALYSIS
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("8E. NULL ANALYSIS")
print("=" * 80)

# Test 23: Critical columns in batches
critical_nulls = batches[['batch_id', 'product_id', 'manufacturer_id', 'mfg_date', 'batch_size_units']].isnull().sum().sum()
log_test('Nulls', 'Critical columns in batches',
         'PASS' if critical_nulls == 0 else 'FAIL', critical_nulls)

# Test 24: Critical columns in orders
critical_nulls = orders[['ERP_EXPORT_ROW_ID', 'CUSTOMER_ID', 'PRODUCT_ID', 'ORDER_DATE', 'ORDER_QTY']].isnull().sum().sum()
log_test('Nulls', 'Critical columns in orders',
         'PASS' if critical_nulls == 0 else 'FAIL', critical_nulls)

# Test 25: Conditional nulls in orders
batch_null_pct = orders['BATCH_ID'].isnull().sum() / len(orders) * 100
print(f" Conditional null check: BATCH_ID null in {batch_null_pct:.1f}% of orders (acceptable)")

# ═══════════════════════════════════════════════════════════════════════
# GENERATE REPORT
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("GENERATING VALIDATION REPORT")
print("=" * 80)

# Create results DataFrame
df_results = pd.DataFrame(results)

# Summary by category
summary = df_results.groupby(['category', 'status']).size().unstack(fill_value=0)
summary['total'] = summary.sum(axis=1)

# Overall status
total_tests = len(results)
passed = len(df_results[df_results['status'] == 'PASS'])
warned = len(df_results[df_results['status'] == 'WARN'])
failed = len(df_results[df_results['status'] == 'FAIL'])

print(f"\nValidation Summary:")
print(f"  Total tests: {total_tests}")
print(f"  Passed: {passed}")
print(f"  Warned: {warned}")
print(f"  Failed: {failed}")

# Overall verdict
if failed == 0 and warned == 0:
    verdict = "READY FOR PHASE 2"
elif failed == 0:
    verdict = "REVIEW WARNINGS, PROCEED WITH CAUTION"
else:
    verdict = "NOT READY - FIX FAILURES BEFORE PHASE 2"

print(f"\n{verdict}\n")

# ═══════════════════════════════════════════════════════════════════════
# EXPORT RESULTS
# ═══════════════════════════════════════════════════════════════════════

output_file = OUTPUT_DIR / 'validation_results.md'

with open(output_file, 'w', encoding="utf-8") as f:
    f.write("# Validation Results\n\n")
    f.write(f"Generated: {datetime.now()}\n\n")
    f.write(f"## Overall Status\n{verdict}\n\n")
    f.write("## Detailed Results\n\n")
    f.write("| Category | Test | Status | Violations | Notes |\n")
    f.write("|----------|------|--------|------------|-------|\n")

    for _, row in df_results.iterrows():
        f.write(
            f"| {row['category']} | "
            f"{row['test']} | "
            f"{row['status']} | "
            f"{row['violations']} | "
            f"{row['notes']} |\n"
        )
           
    f.write("\n\n ## Results by Category\n\n")
    f.write(summary.to_markdown())

print(f"Report saved to: {output_file}")

if failed > 0:
    sys.exit(1)

print("\nValidation complete.\n")