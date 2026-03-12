#!/usr/bin/env python3
"""
PHASE 1 — PRE-INGESTION RAW DATA PROFILING

Table: product_batches.csv
Goal:
- Understand structure
- Discover grain
- Identify candidate keys
- Discover relationships
- Build rough ERD
"""

import pandas as pd
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

batches = pd.read_csv(DATA_DIR / "product_batches.csv")
products = pd.read_csv(DATA_DIR / "products_master.csv")
manufacturers = pd.read_csv(DATA_DIR / "manufacturers.csv")

print("=" * 80)
print("PHASE 1 — RAW DATA PROFILING: product_batches")
print("=" * 80)

# =============================================================================
# STEP 1 — ENTITY INVENTORY
# =============================================================================

print("\nSTEP 1 — ENTITY INVENTORY")
print("-" * 80)

print(f"Rows: {len(batches):,}")
print(f"Columns: {len(batches.columns)}")
print("\nColumn list:")
print(list(batches.columns))

print("\nData types:")
print(batches.dtypes)

# =============================================================================
# STEP 2 — SAMPLE INSPECTION
# =============================================================================

print("\nSTEP 2 — SAMPLE ROWS")
print("-" * 80)

# Columns selected for readability in documentation
sample_cols = [
    "batch_id",
    "batch_number",
    "product_id",
    "manufacturer_id",
    "mfg_date", 
    "expiry_date", 
    "batch_size_units"
]

print("\nFirst 5 rows (selected columns):")
print(batches[sample_cols].head())

print("\nRandom sample (5 rows, selected columns):")
print(batches[sample_cols].sample(5, random_state=42))

# =============================================================================
# STEP 3 — GRAIN EXPLORATION
# =============================================================================

print("\nSTEP 3 — GRAIN EXPLORATION")
print("-" * 80)

print(f"Total rows: {len(batches):,}")
print(f"Unique batch_id: {batches['batch_id'].nunique():,}")
print(f"Unique batch_number: {batches['batch_number'].nunique():,}")
print(f"Unique lot_number: {batches['lot_number'].nunique():,}")

print("\n Initial grain hypothesis:")
print("-> One row per manufacturing batch (batch_id)")

# =============================================================================
# STEP 4 — KEY DISCOVERY
# =============================================================================

print("\nSTEP 4 — KEY DISCOVERY")
print("-" * 80)

candidate_keys = ["batch_id", "batch_number", "lot_number"]

for col in candidate_keys:
    print(
        f"{col:15s} | unique: {batches[col].nunique():,} "
        f"| nulls: {batches[col].isnull().sum():,}"
    )

print("\nLikely PK:")
print("→ batch_id (system-generated surrogate key)")

# =============================================================================
# STEP 5 — RELATIONSHIP DISCOVERY
# =============================================================================

print("\nSTEP 5 — RELATIONSHIP DISCOVERY")
print("-" * 80)

product_overlap = batches["product_id"].isin(products["product_id"]).mean() * 100
manufacturer_overlap = batches["manufacturer_id"].isin(
    manufacturers["manufacturer_id"]
).mean() * 100

print(f"product_id overlap with products_master: {product_overlap:.2f}%")
print(f"manufacturer_id overlap with manufacturers: {manufacturer_overlap:.2f}%")

print("\nExpected relationships:")
print("product_batches.product_id → products_master.product_id (M:1)")
print("product_batches.manufacturer_id → manufacturers.manufacturer_id (M:1)")

# =============================================================================
# STEP 6 — CARDINALITY HINTS
# =============================================================================

print("\nSTEP 6 — CARDINALITY HINTS")
print("-" * 80)

print("Batches per product (top 5):")
print(
    batches.groupby("product_id")
    .size()
    .sort_values(ascending=False)
    .head(5)
)

print("\nBatches per manufacturer (top 5):")
print(
    batches.groupby("manufacturer_id")
    .size()
    .sort_values(ascending=False)
    .head(5)
)

# =============================================================================
# STEP 7 — TEMPORAL COVERAGE
# =============================================================================

print("\nSTEP 7 — TEMPORAL COVERAGE")
print("-" * 80)

batches["mfg_date"] = pd.to_datetime(batches["mfg_date"])
batches["expiry_date"] = pd.to_datetime(batches["expiry_date"])

print(f"Manufacturing date range: {batches['mfg_date'].min()} → {batches['mfg_date'].max()}")
print(f"Expiry date range: {batches['expiry_date'].min()} → {batches['expiry_date'].max()}")

print("\nManufacturing years present:")
print(sorted(batches["mfg_date"].dt.year.unique()))

# =============================================================================
# STEP 8 — MEASURE VS ATTRIBUTE IDENTIFICATION
# =============================================================================

print("\nSTEP 8 — MEASURE VS ATTRIBUTE IDENTIFICATION")
print("-" * 80)

measures = [
    "batch_size_units",
    "unit_mrp"
]

attributes = [
    col for col in batches.columns if col not in measures
]

print("Measures:")
print(measures)

print("\nAttributes:")
print(attributes)

# =============================================================================
# STEP 9 — PHASE 1 OUTPUT SUMMARY
# =============================================================================

print("\nSTEP 9 — PHASE 1 SUMMARY")
print("-" * 80)

print("""
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
""")

print("\nPHASE 1 PROFILING COMPLETE")
print("=" * 80)

# =============================================================================
# SAVE OUTPUT TO MARKDOWN
# =============================================================================

sys.stdout = _tee._stdout  # restore original stdout

_output_md = OUTPUT_DIR / "product_batches_profiling.md"

_md_content = f"""# Phase 1 — Raw Data Profiling: product_batches

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Source table:** `product_batches.csv`  
**Script:** `{Path(__file__).name}`

---

```
{_tee.getvalue()}
```
"""

_output_md.write_text(_md_content, encoding="utf-8")
print(f"\n Profiling output saved → {_output_md.relative_to(PROJECT_ROOT)}")