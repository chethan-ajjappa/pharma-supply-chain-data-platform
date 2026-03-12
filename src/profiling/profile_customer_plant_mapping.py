# customers_plant_mapping.csv - Data Profiling Validation
# Validating manual observations with code

import pandas as pd
import numpy as np
import io
import sys
from pathlib import Path
from datetime import datetime
from collections import Counter

# =============================================================================
# OUTPUT CAPTURE SETUP
# =============================================================================

class Tee:
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
# Base paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent           # parent directory Project root
DATA_DIR = PROJECT_ROOT / 'raw'                     # raw_dataset folder
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "profiling_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(DATA_DIR/ 'customer_plant_mapping.csv')
customers = pd.read_csv(DATA_DIR / "customers.csv")
plant_warehouse = pd.read_csv(DATA_DIR / "plant_warehouse.csv")

print("="*80)
print("CUSTOMER PLANT MAPPING - INITIAL LOAD")
print("="*80)
print(f"Rows: {len(df)}")
print(f"Columns:{len(df.columns)}")
print(f"\n Column list:\n {list(df.columns)}")
print(f"\n Data types:\n{df.dtypes}")


print("\n1. STRUCTURAL VALIDATION")
print("-" * 40)

total_rows = len(df)
grain_dupes = df.duplicated(subset=["CUSTOMER_ID", "PLANT_CODE"]).sum()

print(f"Total rows: {total_rows}")
print(f"Duplicate (CUSTOMER_ID, PLANT_CODE): {grain_dupes}")

assert grain_dupes == 0, "Grain violation: duplicate customer–plant mapping!"

print("\n2. REFERENTIAL INTEGRITY")
print("-" * 40)

invalid_customers = df[~df["CUSTOMER_ID"].isin(customers["CUSTOMER_ID"])]
invalid_plants = df[~df["PLANT_CODE"].isin(plant_warehouse["PLANT_CODE"])]

print(f"Invalid CUSTOMER_ID refs: {len(invalid_customers)}")
print(f"Invalid PLANT_CODE refs: {len(invalid_plants)}")

assert len(invalid_customers) == 0, "Orphan CUSTOMER_ID found!"
assert len(invalid_plants) == 0, "Orphan PLANT_CODE found!"

print("\n3. CATEGORICAL SANITY")
print("-" * 40)

print("priority_flag values:")
print(df["PRIORITY_FLAG"].value_counts(dropna=False))

print("\nactive_flag values:")
print(df["ACTIVE_FLAG"].value_counts(dropna=False))


print("\n4A. BUSINESS RULE: ONE PRIMARY PER CUSTOMER")
print("-" * 40)

primary_counts = (
    df[df["PRIORITY_FLAG"] == "PRIMARY"]
    .groupby("CUSTOMER_ID")
    .size()
)
# Customers with more than one PRIMARY
violations = primary_counts[primary_counts > 1]

# Customers with NO PRIMARY plant
customers_without_primary = set(customers["CUSTOMER_ID"]) - set(primary_counts.index)

print(f"Customers without PRIMARY plant: {len(customers_without_primary)}")

print(f"Customers with >1 PRIMARY plant: {len(violations)}")
assert len(violations) == 0, "Multiple PRIMARY plants for same customer!"


print("\n4B. CUSTOMER COVERAGE SUMMARY")
print("-" * 40)

total_customers = df["CUSTOMER_ID"].nunique()
customers_with_primary = df[df["PRIORITY_FLAG"] == "PRIMARY"]["CUSTOMER_ID"].nunique()
customers_with_secondary = df[df["PRIORITY_FLAG"] == "SECONDARY"]["CUSTOMER_ID"].nunique()
customers_with_backup = df[df["PRIORITY_FLAG"] == "BACKUP"]["CUSTOMER_ID"].nunique()

print(f"Total customers mapped: {total_customers}")
print(f"Customers with PRIMARY plant: {customers_with_primary}")
print(f"Customers with SECONDARY plant: {customers_with_secondary}")
print(f"Customers with BACKUP plant: {customers_with_backup}")


# =============================================================================
# SAVE OUTPUT TO MARKDOWN
# =============================================================================
sys.stdout = _tee._stdout

_output_md = OUTPUT_DIR / "customer_plant_mapping.md"

_md_content = f""" # Phase 1 - Raw data profiling: customer plant mapping

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Source table:** `Customer_plant_mapping.csv`
**Script:** `{Path(__file__).name}`

---

```
{_tee.getvalue()}
```

"""
_output_md.write_text(_md_content, encoding="utf-8")
print(f"\n Profiling output saved -> {_output_md.relative_to(PROJECT_ROOT)}")