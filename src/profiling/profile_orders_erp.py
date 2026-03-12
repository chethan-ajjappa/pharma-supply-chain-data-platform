#!/usr/bin/env python3
"""
PHASE 1 — PRE-INGESTION RAW DATA PROFILING

Table: Orders_ERP (2022)

Goal:
- Understand structure and grain
- Validate accumulating snapshot pattern
- Identify primary/business keys
- Validate relationships with upstream tables
- Check lifecycle consistency (order → delivery → invoice)
- Validate price/quantity waterfalls
- Prepare modeling assumptions
"""

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
DATA_DIR = PROJECT_ROOT / "raw"
ORDER_DIR = DATA_DIR / "orders_erp"

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "profiling_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load 2022 as representative year
orders = pd.read_csv(ORDER_DIR / "Orders_ERP_2022.csv")

# Load reference tables for FK validation
customers = pd.read_csv(DATA_DIR / "customers.csv")
products = pd.read_csv(DATA_DIR / "products_master.csv")
plants = pd.read_csv(DATA_DIR / "plant_warehouse.csv")
batches = pd.read_csv(DATA_DIR / "product_batches.csv")

print("=" * 80)
print("PHASE 1 — RAW DATA PROFILING: Orders_ERP (2022)")
print("=" * 80)

# =============================================================================
# STEP 1 — ENTITY INVENTORY
# =============================================================================

print("\nSTEP 1 — ENTITY INVENTORY")
print("-" * 80)

print(f"Rows: {len(orders):,}")
print(f"Columns: {len(orders.columns)}")

print("\nColumn list:")
print(list(orders.columns))

print("\nData types:")
print(orders.dtypes)

print("\nMemory usage:")
print(f"{orders.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# =============================================================================
# STEP 2 — SAMPLE INSPECTION
# =============================================================================

print("\nSTEP 2 — SAMPLE ROWS")
print("-" * 80)

print("\nFirst 5 rows:")
print(orders.head(5))

print("\nLast 5 rows:")
print(orders.tail(5))

print("\nRandom sample (5 rows):")
print(orders.sample(5, random_state=42))

# =============================================================================
# STEP 3 — GRAIN EXPLORATION
# =============================================================================

print("\nSTEP 3 — GRAIN EXPLORATION")
print("-" * 80)

print(f"Total rows: {len(orders):,}")
print(f"Unique ORDER_LINE_ID: {orders['ORDER_LINE_ID'].nunique():,}")
print(f"Unique ORDER_NO: {orders['ORDER_NO'].nunique():,}")
print(f"Unique ERP_EXPORT_ROW_ID: {orders['ERP_EXPORT_ROW_ID'].nunique():,}")

# Business key uniqueness
business_key_unique = (
    orders[["ORDER_NO", "ORDER_LINE_NO"]]
    .drop_duplicates()
    .shape[0]
)
print(f"\nUnique (ORDER_NO + ORDER_LINE_NO): {business_key_unique:,}")

print("\nSTEP 3A — GRAIN HYPOTHESIS VALIDATION")
print("-" * 80)

print("Checking if ORDER_LINE_ID represents physical grain...")

rows_per_line = orders.groupby("ORDER_LINE_ID").size()

print(f"Min rows per ORDER_LINE_ID: {rows_per_line.min()}")
print(f"Max rows per ORDER_LINE_ID: {rows_per_line.max()}")
print(f"Avg rows per ORDER_LINE_ID: {rows_per_line.mean():.2f}")

if rows_per_line.max() > 1:
    print("ORDER_LINE_ID is NOT the physical grain")
else:
    print("ORDER_LINE_ID is the physical grain")

print("\nSTEP 3B — STATUS MULTIPLICITY ANALYSIS")
print("-" * 80)

status_counts = orders.groupby("ORDER_LINE_ID")["LINE_STATUS"].nunique()

print(status_counts.describe())

print("\nChecking (ORDER_LINE_ID × LINE_STATUS) uniqueness...")

violations = (
    orders.groupby(["ORDER_LINE_ID", "LINE_STATUS"])
    .size()
    .reset_index(name="row_count")
    .query("row_count > 1")
)

print(f"Violations found: {len(violations):,}")

if len(violations) > 0:
    print("LINE_STATUS does not define unique lifecycle events")
else:
    print("LINE_STATUS uniquely identifies lifecycle events")

print("\nSTEP 3C — PHYSICAL GRAIN IDENTIFICATION")
print("-" * 80)

export_key_unique = orders["ERP_EXPORT_ROW_ID"].is_unique

print(f"ERP_EXPORT_ROW_ID unique: {export_key_unique}")

if export_key_unique:
    print("Observed physical grain:")
    print("→ One row per export record")
    print("→ Primary Key: ERP_EXPORT_ROW_ID")
else:
    print("No stable physical grain identified")

# =============================================================================
# STEP 4 — PRIMARY KEY DISCOVERY (POST-GRAIN CONFIRMATION)
# =============================================================================

print("\nSTEP 4 — PRIMARY KEY DISCOVERY")
print("-" * 80)

candidate_identifiers = [
    "ERP_EXPORT_ROW_ID",
    "ORDER_LINE_ID",
    "ORDER_NO",
    "SO_NUMBER"
]

for col in candidate_identifiers:
    print(
        f"{col:25s} | unique: {orders[col].nunique():,} "
        f"| nulls: {orders[col].isnull().sum():,} "
        f"| unique_ratio: {orders[col].nunique() / len(orders):.3f}"
    )

# Composite business identifier
print(
    f"\n{'ORDER_NO + ORDER_LINE_NO':25s} | "
    f"unique: {business_key_unique:,} "
    f"| avg_rows_per_key: {len(orders) / business_key_unique:.2f}"
)

print("\nKey classification:")
print("→ Physical Primary Key      : ERP_EXPORT_ROW_ID (1 row = 1 export event)")
print("→ Logical Order Line ID     : ORDER_LINE_ID (repeats across lifecycle events)")
print("→ Business Identifier       : ORDER_NO + ORDER_LINE_NO (non-unique by design)")
print("→ Snapshot-style PKs        : ❌ Not applicable for this dataset")


# =============================================================================
# STEP 5 — RELATIONSHIP DISCOVERY
# =============================================================================

print("\nSTEP 5 — RELATIONSHIP DISCOVERY")
print("-" * 80)

# FK overlap checks
customer_overlap = orders["CUSTOMER_ID"].isin(customers["CUSTOMER_ID"]).mean() * 100
product_overlap = orders["PRODUCT_ID"].isin(products["product_id"]).mean() * 100
plant_overlap = orders["PLANT_CODE"].isin(plants["PLANT_ID"]).mean() * 100

# Batch overlap (only for non-null BATCH_ID)
batch_orders = orders[orders["BATCH_ID"].notna()]
if len(batch_orders) > 0:
    batch_overlap = batch_orders["BATCH_ID"].isin(batches["batch_id"]).mean() * 100
else:
    batch_overlap = np.nan

print(f"CUSTOMER_ID overlap with customers: {customer_overlap:.2f}%")
print(f"PRODUCT_ID overlap with products_master: {product_overlap:.2f}%")
print(f"PLANT_ID overlap with plant_warehouse: {plant_overlap:.2f}%")
print(f"BATCH_ID overlap with product_batches: {batch_overlap:.2f}% (where not null)")

print(f"\nBATCH_ID null count: {orders['BATCH_ID'].isnull().sum():,} / {len(orders):,}")
print(f"BATCH_ID null percentage: {orders['BATCH_ID'].isnull().mean()*100:.1f}%")

print("\nExpected relationships:")
print("Orders_ERP.CUSTOMER_ID → customers.CUSTOMER_ID (M:1)")
print("Orders_ERP.PRODUCT_ID → products_master.product_id (M:1)")
print("Orders_ERP.PLANT_CODE → plant_warehouse.PLANT_ID (M:1)")
print("Orders_ERP.BATCH_ID → product_batches.batch_id (M:1, conditional)")

# =============================================================================
# STEP 6 — LIFECYCLE STAGE ANALYSIS
# =============================================================================

print("\nSTEP 6 — LIFECYCLE STAGE ANALYSIS")
print("-" * 80)

print("LINE_STATUS distribution:")
print(orders["LINE_STATUS"].value_counts())

print("\nORDER_STATUS distribution:")
print(orders["ORDER_STATUS"].value_counts())

print("\nLifecycle stage indicators:")
print(f"Rows with ORDER_QTY > 0: {(orders['ORDER_QTY'] > 0).sum():,}")
print(f"Rows with DELIVERY_QTY > 0: {(orders['DELIVERY_QTY'] > 0).sum():,}")
print(f"Rows with INVOICE_QTY > 0: {(orders['INVOICE_QTY'] > 0).sum():,}")

print("\nDocument number population:")
print(f"DELIVERY_NO populated: {orders['DELIVERY_NO'].notna().sum():,}")
print(f"INVOICE_NO populated: {orders['INVOICE_NO'].notna().sum():,}")

print("\nDate field population:")
print(f"ORDER_DATE null count: {orders['ORDER_DATE'].isnull().sum():,}")
print(f"ACTUAL_DELIVERY_DATE null count: {orders['ACTUAL_DELIVERY_DATE'].isnull().sum():,}")
print(f"INVOICE_DATE null count: {orders['INVOICE_DATE'].isnull().sum():,}")

# =============================================================================
# STEP 7 — QUANTITY WATERFALL VALIDATION
# =============================================================================

print("\nSTEP 7 — QUANTITY WATERFALL VALIDATION")
print("-" * 80)

print("Quantity statistics:")
print(orders[["ORDER_QTY", "DELIVERY_QTY", "INVOICE_QTY"]].describe())

# Waterfall validation
orders["qty_waterfall_valid"] = (
    (orders["ORDER_QTY"] >= orders["DELIVERY_QTY"]) &
    (orders["DELIVERY_QTY"] >= orders["INVOICE_QTY"])
)

waterfall_violations = (~orders["qty_waterfall_valid"]).sum()
print(f"\nQuantity waterfall violations: {waterfall_violations:,} / {len(orders):,}")

if waterfall_violations > 0:
    print("\nSample violations:")
    print(orders[~orders["qty_waterfall_valid"]][
        ["ORDER_LINE_ID", "ORDER_QTY", "DELIVERY_QTY", "INVOICE_QTY", "LINE_STATUS"]
    ].head(10))

# Fulfillment metrics
delivered_lines = orders[orders["DELIVERY_QTY"] > 0]
if len(delivered_lines) > 0:
    fulfillment_rate = (delivered_lines["DELIVERY_QTY"] / delivered_lines["ORDER_QTY"]).mean()
    print(f"\nAverage fulfillment rate (delivered lines): {fulfillment_rate*100:.2f}%")

invoiced_lines = orders[orders["INVOICE_QTY"] > 0]
if len(invoiced_lines) > 0:
    invoice_rate = (invoiced_lines["INVOICE_QTY"] / invoiced_lines["DELIVERY_QTY"]).mean()
    print(f"Average invoice rate (invoiced lines): {invoice_rate*100:.2f}%")

# =============================================================================
# STEP 8 — PRICE VALIDATION
# =============================================================================

print("\nSTEP 8 — PRICE VALIDATION")
print("-" * 80)

print("Price statistics:")
print(orders[["MRP", "FLOOR_PRICE", "ORDER_PRICE", "DELIVERY_PRICE", "INVOICE_PRICE"]].describe())

# Price bounds check (ORDER_PRICE should be between FLOOR_PRICE and MRP * ORDER_QTY)
# Note: ORDER_PRICE is total line amount, so we need to compare with MRP * ORDER_QTY
orders["price_within_bounds"] = (
    (orders["ORDER_PRICE"] >= orders["FLOOR_PRICE"] * orders["ORDER_QTY"] * 0.95) &  # 5% tolerance
    (orders["ORDER_PRICE"] <= orders["MRP"] * orders["ORDER_QTY"] * 1.05)
)

price_violations = (~orders["price_within_bounds"]).sum()
print(f"\nPrice bound violations (5% tolerance): {price_violations:,} / {len(orders):,}")

if price_violations > 0:
    print("\nSample price violations:")
    print(orders[~orders["price_within_bounds"]][
        ["ORDER_LINE_ID", "ORDER_QTY", "MRP", "FLOOR_PRICE", "ORDER_PRICE"]
    ].head(10))

# GST calculation check
orders["gst_calculated"] = orders["NET_LINE_AMOUNT"] * (orders["GST_RATE"] / 100)
orders["gst_match"] = np.isclose(
    orders["GST_AMOUNT"].fillna(0),
    orders["gst_calculated"].fillna(0),
    rtol=0.01  # 1% tolerance
)

gst_mismatches = (~orders["gst_match"]).sum()
print(f"\nGST calculation mismatches: {gst_mismatches:,} / {len(orders):,}")

# Total amount check
orders["total_calculated"] = orders["NET_LINE_AMOUNT"] + orders["GST_AMOUNT"]
orders["total_match"] = np.isclose(
    orders["TOTAL_LINE_AMOUNT_INCL_GST"].fillna(0),
    orders["total_calculated"].fillna(0),
    rtol=0.01
)

total_mismatches = (~orders["total_match"]).sum()
print(f"Total amount calculation mismatches: {total_mismatches:,} / {len(orders):,}")

# =============================================================================
# STEP 9 — TEMPORAL ANALYSIS
# =============================================================================

print("\nSTEP 9 — TEMPORAL ANALYSIS")
print("-" * 80)

# Convert date columns
date_cols = ["ORDER_DATE", "EXPECTED_DELIVERY_DATE", "ACTUAL_DELIVERY_DATE", "INVOICE_DATE"]
for col in date_cols:
    orders[col] = pd.to_datetime(orders[col], errors='coerce')

print("Date ranges:")
print(f"ORDER_DATE: {orders['ORDER_DATE'].min()} → {orders['ORDER_DATE'].max()}")
print(f"ACTUAL_DELIVERY_DATE: {orders['ACTUAL_DELIVERY_DATE'].min()} → {orders['ACTUAL_DELIVERY_DATE'].max()}")
print(f"INVOICE_DATE: {orders['INVOICE_DATE'].min()} → {orders['INVOICE_DATE'].max()}")

# Date progression validation (where dates exist)
delivered_orders = orders[orders["ACTUAL_DELIVERY_DATE"].notna()].copy()
if len(delivered_orders) > 0:
    delivered_orders["delivery_lag_days"] = (
        delivered_orders["ACTUAL_DELIVERY_DATE"] - delivered_orders["ORDER_DATE"]
    ).dt.days

    print(f"\nDelivery lag statistics (n={len(delivered_orders):,}):")
    print(delivered_orders["delivery_lag_days"].describe())

    # Check for invalid date progressions
    invalid_delivery_dates = (delivered_orders["ACTUAL_DELIVERY_DATE"] < delivered_orders["ORDER_DATE"]).sum()
    print(f"Invalid delivery dates (before order date): {invalid_delivery_dates:,}")

invoiced_orders = orders[orders["INVOICE_DATE"].notna()].copy()
if len(invoiced_orders) > 0:
    invoiced_orders["invoice_lag_days"] = (
        invoiced_orders["INVOICE_DATE"] - invoiced_orders["ACTUAL_DELIVERY_DATE"]
    ).dt.days
    print(f"\nInvoice lag statistics (n={len(invoiced_orders):,}):")
    print(invoiced_orders["invoice_lag_days"].describe())

    invalid_invoice_dates = (invoiced_orders["INVOICE_DATE"] < invoiced_orders["ACTUAL_DELIVERY_DATE"]).sum()
    print(f"Invalid invoice dates (before delivery date): {invalid_invoice_dates:,}")

orders["order_year_extracted"] = orders["ORDER_DATE"].dt.year
year_mismatch = (orders["ORDER_YEAR"] != orders["order_year_extracted"]).sum()
print(f"\nORDER_YEAR mismatch with ORDER_DATE: {year_mismatch:,} / {len(orders):,}")

# =============================================================================
# STEP 10 — BATCH LINKAGE ANALYSIS
# =============================================================================

print("\nSTEP 10 — BATCH LINKAGE ANALYSIS")
print("-" * 80)

# Hypothesis: BATCH_ID populated only when DELIVERY_QTY > 0
batch_populated = orders["BATCH_ID"].notna()
delivery_executed = orders["DELIVERY_QTY"] > 0
print("Batch linkage patterns:")
print(f"Total rows: {len(orders):,}")
print(f"Rows with BATCH_ID: {batch_populated.sum():,}")
print(f"Rows with DELIVERY_QTY > 0: {delivery_executed.sum():,}")

# Cross-tabulation
batch_delivery_cross = pd.crosstab(
    batch_populated,
    delivery_executed,
    rownames=["BATCH_ID populated"],
    colnames=["DELIVERY_QTY > 0"]
)
print("\nBatch-Delivery crosstab:")
print(batch_delivery_cross)

# Expected pattern: BATCH_ID populated ↔ DELIVERY_QTY > 0
expected_pattern = (batch_populated == delivery_executed).sum()
print(f"\nRows matching expected pattern: {expected_pattern:,} / {len(orders):,}")
unexpected_batch_without_delivery = (batch_populated & ~delivery_executed).sum()
unexpected_delivery_without_batch = (~batch_populated & delivery_executed).sum()
print(f"Unexpected: BATCH_ID but no delivery: {unexpected_batch_without_delivery:,}")
print(f"Unexpected: Delivery but no BATCH_ID: {unexpected_delivery_without_batch:,}")

# =============================================================================
# STEP 11 — STATUS CONSISTENCY VALIDATION
# =============================================================================

print("\nSTEP 11 — STATUS CONSISTENCY VALIDATION")
print("-" * 80)

# LINE_STATUS vs quantities
print("LINE_STATUS vs DELIVERY_QTY:")
status_delivery = orders.groupby("LINE_STATUS").agg({
    "DELIVERY_QTY": ["count", lambda x: (x > 0).sum()]
})
status_delivery.columns = ["Total_Lines", "Lines_with_Delivery"]
print(status_delivery)

print("\nLINE_STATUS vs INVOICE_QTY:")
status_invoice = orders.groupby("LINE_STATUS").agg({
    "INVOICE_QTY": ["count", lambda x: (x > 0).sum()]
})
status_invoice.columns = ["Total_Lines", "Lines_with_Invoice"]
print(status_invoice)

# Inconsistency checks
inconsistent_ordered = (
    (orders["LINE_STATUS"] == "Ordered") &
    (orders["DELIVERY_QTY"] > 0)
).sum()
print(f"\nInconsistent: LINE_STATUS='Ordered' but DELIVERY_QTY > 0: {inconsistent_ordered:,}")

inconsistent_delivered = (
    (orders["LINE_STATUS"] == "Delivered") &
    (orders["DELIVERY_QTY"] == 0)
).sum()
print(f"Inconsistent: LINE_STATUS='Delivered' but DELIVERY_QTY = 0: {inconsistent_delivered:,}")

# =============================================================================
# STEP 12 — PAYMENT MODE & STATUS ANALYSIS
# =============================================================================

print("\nSTEP 12 — PAYMENT MODE & STATUS ANALYSIS")
print("-" * 80)

print("PAYMENT_MODE distribution:")
print(orders["PAYMENT_MODE"].value_counts())

print("\nPAYMENT_STATUS distribution:")
print(orders["PAYMENT_STATUS"].value_counts())

print("\nPAYMENT_MODE vs PAYMENT_STATUS crosstab:")
payment_cross = pd.crosstab(
    orders["PAYMENT_MODE"],
    orders["PAYMENT_STATUS"],
    margins=True
)
print(payment_cross)

# =============================================================================
# STEP 13 — CANCELLATION & RETURN ANALYSIS
# =============================================================================

print("\nSTEP 13 — CANCELLATION & RETURN ANALYSIS")
print("-" * 80)

cancelled_lines = (orders["LINE_STATUS"] == "Cancelled").sum()
returned_lines = (orders["LINE_STATUS"] == "Returned").sum()
print(f"Cancelled lines: {cancelled_lines:,}")
print(f"Returned lines: {returned_lines:,}")

cancel_reason_populated = orders["CANCEL_REASON"].notna().sum()
return_reason_populated = orders["RETURN_REASON"].notna().sum()
rma_populated = orders["RMA_NO"].notna().sum()
print(f"\nCANCEL_REASON populated: {cancel_reason_populated:,}")
print(f"RETURN_REASON populated: {return_reason_populated:,}")
print(f"RMA_NO populated: {rma_populated:,}")

if cancelled_lines > 0:
    print("\nTop cancellation reasons:")
    print(orders[orders["CANCEL_REASON"].notna()]["CANCEL_REASON"].value_counts().head(10))

if returned_lines > 0:
    print("\nTop return reasons:")
    print(orders[orders["RETURN_REASON"].notna()]["RETURN_REASON"].value_counts().head(10))

# =============================================================================
# STEP 14 — ORDER HEADER AGGREGATION
# =============================================================================

print("\nSTEP 14 — ORDER HEADER AGGREGATION")
print("-" * 80)

order_headers = orders.groupby("ORDER_NO").agg({
    "ORDER_LINE_NO": "count",
    "CUSTOMER_ID": "nunique",
    "ORDER_DATE": "nunique",
    "PLANT_CODE": "nunique",
    "ORDER_QTY": "sum",
    "ORDER_PRICE": "sum"
}).rename(columns={
    "ORDER_LINE_NO": "line_count",
    "CUSTOMER_ID": "unique_customers",
    "ORDER_DATE": "unique_dates",
    "PLANT_CODE": "unique_plants",
    "ORDER_QTY": "total_qty",
    "ORDER_PRICE": "total_amount"
})

print("Order header summary:")
print(order_headers.describe())

# Validation: Each order should have consistent CUSTOMER_ID and ORDER_DATE
inconsistent_orders = (
    (order_headers["unique_customers"] > 1) |
    (order_headers["unique_dates"] > 1)
).sum()
print(f"\nOrders with inconsistent CUSTOMER_ID or ORDER_DATE: {inconsistent_orders:,}")

if inconsistent_orders > 0:
    print("\nSample inconsistent orders:")
    print(order_headers[
        (order_headers["unique_customers"] > 1) |
        (order_headers["unique_dates"] > 1)
    ].head(10))

# Multi-plant orders
multi_plant_orders = (order_headers["unique_plants"] > 1).sum()
print(f"\nOrders spanning multiple plants: {multi_plant_orders:,} / {len(order_headers):,}")

# =============================================================================
# STEP 15 — COMPLETENESS ASSESSMENT
# =============================================================================

print("\nSTEP 15 — COMPLETENESS ASSESSMENT")
print("-" * 80)

print("Null counts for critical columns:")
critical_cols = [
    "ORDER_LINE_ID", "ORDER_NO", "ORDER_LINE_NO",
    "CUSTOMER_ID", "PRODUCT_ID", "PLANT_CODE",
    "ORDER_DATE", "ORDER_QTY", "ORDER_PRICE", "ORDER_YEAR"
]
for col in critical_cols:
    null_count = orders[col].isnull().sum()
    null_pct = null_count / len(orders) * 100
    print(f"{col:25s}: {null_count:6,} ({null_pct:5.2f}%)")

print("\nNull counts for conditional columns:")
conditional_cols = [
    "BATCH_ID", "DELIVERY_NO", "INVOICE_NO",
    "ACTUAL_DELIVERY_DATE", "INVOICE_DATE",
    "DELIVERY_QTY", "INVOICE_QTY",
    "CANCEL_REASON", "RETURN_REASON", "RMA_NO"
]
for col in conditional_cols:
    null_count = orders[col].isnull().sum()
    null_pct = null_count / len(orders) * 100
    print(f"{col:25s}: {null_count:6,} ({null_pct:5.2f}%)")

print("\nDATA QUALITY ASSESSMENT SNAPSHOT")
print("-" * 80)

dq_summary = {
    "table": "Orders_ERP_2022",
    "rows": len(orders),
    "physical_grain": "ERP_EXPORT_ROW_ID",
    "physical_grain_confirmed": orders["ERP_EXPORT_ROW_ID"].is_unique,
    "physical_pk_violations": len(orders) - orders["ERP_EXPORT_ROW_ID"].nunique(),
    "grain_confirmed": orders["ERP_EXPORT_ROW_ID"].nunique() == len(orders),
    "pk_violations": len(orders) - orders["ERP_EXPORT_ROW_ID"].nunique(),
    "business_key_violations": len(orders) - business_key_unique,
    "fk_customer_overlap_pct": round(customer_overlap, 2),
    "fk_product_overlap_pct": round(product_overlap, 2),
    "fk_plant_overlap_pct": round(plant_overlap, 2),
    "qty_waterfall_violations": int(waterfall_violations),
    "price_violations": int(price_violations),
    "gst_mismatches": int(gst_mismatches),
    "date_progression_violations": int(invalid_delivery_dates + invalid_invoice_dates),
    "unexpected_batch_without_delivery": int(unexpected_batch_without_delivery),
    "unexpected_delivery_without_batch": int(unexpected_delivery_without_batch),
    "critical_nulls_present": any(
        orders[col].isnull().any()
        for col in ["ORDER_LINE_ID", "ORDER_NO", "CUSTOMER_ID", "PRODUCT_ID"]
    )
}

dq_df = pd.DataFrame([dq_summary])
print(dq_df.T)

print("\nMODELING READINESS VERDICT:")

if (
    dq_summary["physical_grain_confirmed"] and
    dq_summary["pk_violations"] == 0 and
    not dq_summary["critical_nulls_present"] and
    dq_summary["qty_waterfall_violations"] == 0
):
    print("READY FOR DIMENSIONAL MODELING (Accumulating Snapshot Fact)")
else:
    print("NOT READY — CRITICAL DATA QUALITY ISSUES PRESENT")

# =============================================================================
# SAVE OUTPUT TO MARKDOWN
# =============================================================================

sys.stdout = _tee._stdout  # restore original stdout

_output_md = OUTPUT_DIR / "orders_erp_2022_profiling.md"

_md_content = f"""# Phase 1 — Raw Data Profiling: Orders_ERP (2022)

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Source table:** `Orders_ERP_2022.csv`  
**Script:** `{Path(__file__).name}`

---

```
{_tee.getvalue()}
```
"""

_output_md.write_text(_md_content, encoding="utf-8")
print(f"\n Profiling output saved -> {_output_md.relative_to(PROJECT_ROOT)}")