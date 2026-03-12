#!/usr/bin/env python3
"""
PHASE 1 — PRE-INGESTION RAW DATA PROFILING

Table: inventory_movements.csv

Goal:
- Understand structure
- Discover grain
- Identify primary key
- Validate relationships with upstream facts
- Validate movement semantics (IN / OUT pattern)
- Prepare rough modeling assumptions (no balances yet)
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
DATA_DIR = PROJECT_ROOT / "raw"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "profiling_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

mov = pd.read_csv(DATA_DIR / "Inventorymovements.csv")
batches = pd.read_csv(DATA_DIR / "product_batches.csv")
alloc = pd.read_csv(DATA_DIR / "BatchAllocatorCentral.csv")
products = pd.read_csv(DATA_DIR / "products_master.csv")
manufacturers = pd.read_csv(DATA_DIR / "manufacturers.csv")

print("=" * 80)
print("PHASE 1 — RAW DATA PROFILING: inventory_movements")
print("=" * 80)

# =============================================================================
# STEP 1 — ENTITY INVENTORY
# =============================================================================

print("\nSTEP 1 — ENTITY INVENTORY")
print("-" * 80)

print(f"Rows: {len(mov):,}")
print(f"Columns: {len(mov.columns)}")

print("\nColumn list:")
print(list(mov.columns))

print("\nData types:")
print(mov.dtypes)

# =============================================================================
# STEP 2 — SAMPLE INSPECTION
# =============================================================================

print("\nSTEP 2 — SAMPLE ROWS")
print("-" * 80)

# Columns selected for readability in documentation
sample_cols = [
    "movement_id",
    "movement_type",
    "batch_id",
    "allocation_id",
    "product_id",
    "from_plant_id",
    "to_plant_id",
    "units",
    "movement_date"
]

print("\nFirst 5 rows (selected columns):")
print(mov[sample_cols].head().to_markdown(index=False))

print("\nRandom sample (5 rows, selected columns):")
print(mov[sample_cols].sample(5, random_state=42).to_markdown(index=False))

# =============================================================================
# STEP 3 — GRAIN EXPLORATION
# =============================================================================

print("\nSTEP 3 — GRAIN EXPLORATION")
print("-" * 80)

print(f"Total rows: {len(mov):,}")
print(f"Unique movement_id: {mov['movement_id'].nunique():,}")
print(f"Unique batch_id: {mov['batch_id'].nunique():,}")
print(f"Unique allocation_id: {mov['allocation_id'].nunique():,}")

print("\nComposite grain check (batch_id + allocation_id + movement_date):")
composite_unique = (
    mov[["batch_id", "allocation_id", "movement_date"]]
    .drop_duplicates()
    .shape[0]
)
print(f"Unique composite rows: {composite_unique:,}")

print("\nInitial grain hypothesis:")
print("→ One row per inventory movement event")

# =============================================================================
# STEP 4 — PRIMARY KEY DISCOVERY
# =============================================================================

print("\nSTEP 4 — PRIMARY KEY DISCOVERY")
print("-" * 80)

candidate_keys = [
    "movement_id",
    "batch_id",
    "allocation_id"
]

for col in candidate_keys:
    print(
        f"{col:20s} | unique: {mov[col].nunique():,} "
        f"| nulls: {mov[col].isnull().sum():,}"
    )

print("\nLikely PK:")
print("→ movement_id (system-generated surrogate key)")

# =============================================================================
# STEP 5 — RELATIONSHIP DISCOVERY
# =============================================================================

print("\nSTEP 5 — RELATIONSHIP DISCOVERY")
print("-" * 80)

batch_overlap = mov["batch_id"].isin(batches["batch_id"]).mean() * 100
alloc_overlap = mov["allocation_id"].isin(alloc["allocation_id"]).mean() * 100
product_overlap = mov["product_id"].isin(products["product_id"]).mean() * 100
manufacturer_overlap = mov["manufacturer_id"].isin(
    manufacturers["manufacturer_id"]
).mean() * 100

print(f"batch_id overlap with product_batches: {batch_overlap:.2f}%")
print(f"allocation_id overlap with batch_allocator_central: {alloc_overlap:.2f}%")
print(f"product_id overlap with products_master: {product_overlap:.2f}%")
print(f"manufacturer_id overlap with manufacturers: {manufacturer_overlap:.2f}%")

print("\nExpected relationships:")
print("inventory_movements.batch_id → product_batches.batch_id (M:1)")
print("inventory_movements.allocation_id → batch_allocator_central.allocation_id (M:1)")
print("inventory_movements.product_id → products_master.product_id (M:1)")
print("inventory_movements.manufacturer_id → manufacturers.manufacturer_id (M:1)")

# =============================================================================
# STEP 6 — MOVEMENT TYPE & LOCATION SEMANTICS
# =============================================================================

print("\nSTEP 6 — MOVEMENT SEMANTICS")
print("-" * 80)

print("Movement type distribution:")
print(mov["movement_type"].value_counts())

print("\nNull analysis for location columns:")
print(
    mov[["from_plant_id", "to_plant_id"]]
    .isnull()
    .sum()
)

print("\nSample movement_type vs from/to plant presence:")
sample_semantics = (
    mov.assign(
        from_null=mov["from_plant_id"].isnull(),
        to_null=mov["to_plant_id"].isnull()
    )
    .groupby(["movement_type", "from_null", "to_null"])
    .size()
    .reset_index(name="rows")
)
print(sample_semantics.head(10))

# =============================================================================
# STEP 7 — CARDINALITY HINTS
# =============================================================================

print("\nSTEP 7 — CARDINALITY HINTS")
print("-" * 80)

print("Movements per batch (top 10):")
print(
    mov.groupby("batch_id")
    .size()
    .sort_values(ascending=False)
    .head(10)
)

print("\nMovements per allocation (top 10):")
print(
    mov.groupby("allocation_id")
    .size()
    .sort_values(ascending=False)
    .head(10)
)

# =============================================================================
# STEP 8 — TEMPORAL COVERAGE
# =============================================================================

print("\nSTEP 8 — TEMPORAL COVERAGE")
print("-" * 80)

mov["movement_date"] = pd.to_datetime(mov["movement_date"])
mov["mfg_date"] = pd.to_datetime(mov["mfg_date"])
mov["expiry_date"] = pd.to_datetime(mov["expiry_date"])

print(
    f"Movement date range: "
    f"{mov['movement_date'].min()} → {mov['movement_date'].max()}"
)

print("\nMovement years present:")
print(sorted(mov["movement_date"].dt.year.unique()))

# =============================================================================
# STEP 9 — MEASURE VS ATTRIBUTE IDENTIFICATION
# =============================================================================

print("\nSTEP 9 — MEASURE VS ATTRIBUTE IDENTIFICATION")
print("-" * 80)

measures = ["units"]
attributes = [col for col in mov.columns if col not in measures]

print("Measures:")
print(measures)

print("\nAttributes:")
print(attributes)

# =============================================================================
# STEP 10 — PHASE 1 SUMMARY
# =============================================================================

print("\nSTEP 10 — PHASE 1 SUMMARY")
print("-" * 80)

print("""
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
""")

print("\nPHASE 1 PROFILING COMPLETE")
print("=" * 80)

# =============================================================================
# SAVE OUTPUT TO MARKDOWN
# =============================================================================

sys.stdout = _tee._stdout  # restore original stdout

_output_md = OUTPUT_DIR / "inventory_movements_profiling.md"

_md_content = f"""# Phase 1 — Raw Data Profiling: inventory_movements

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Source table:** `Inventorymovements.csv`  
**Script:** `{Path(__file__).name}`

---

```
{_tee.getvalue()}
```
"""

_output_md.write_text(_md_content, encoding="utf-8")
print(f"\n Profiling output saved → {_output_md.relative_to(PROJECT_ROOT)}")