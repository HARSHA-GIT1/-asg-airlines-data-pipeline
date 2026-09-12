"""
ASG Airlines — Task 7b Gold KPI Summary
=================================================
Target Table: ADLS Gen2 'gold/gold_kpi_summary'
Builds a single-row summary table for Power BI high-level cards.
"""

import os
from datetime import datetime, timezone
import pandas as pd
from deltalake import write_deltalake, DeltaTable

STORAGE_ACCOUNT_NAME = "stasgairlines01"
CONTAINER_GOLD       = "gold"
STORAGE_KEY = os.environ.get("ADLS_STORAGE_KEY", "")
if not STORAGE_KEY:
    try:
        import subprocess
        res = subprocess.run(
            ["az", "storage", "account", "keys", "list",
             "--account-name", STORAGE_ACCOUNT_NAME,
             "--resource-group", "rg-asg-airlines",
             "--query", "[0].value", "-o", "tsv"],
            capture_output=True, text=True, check=True
        )
        STORAGE_KEY = res.stdout.strip()
    except Exception:
        raise RuntimeError("ADLS_STORAGE_KEY not set and could not fetch.")

so = {
    "azure_storage_account_name": STORAGE_ACCOUNT_NAME,
    "azure_storage_access_key": STORAGE_KEY,
}

print("=" * 80)
print("ASG AIRLINES — TASK 7b: GOLD KPI SUMMARY")
print("=" * 80)

# Read Gold tables
print("\n[1/3] Reading Gold tables...")
fact_flights = DeltaTable(f"az://{CONTAINER_GOLD}/fact_flights", storage_options=so).to_pandas()
dim_airline  = DeltaTable(f"az://{CONTAINER_GOLD}/dim_airline", storage_options=so).to_pandas()
dim_route    = DeltaTable(f"az://{CONTAINER_GOLD}/dim_route", storage_options=so).to_pandas()

# Calculate Metrics
print("[2/3] Calculating metrics...")
total_flights          = len(fact_flights)
avg_duration_minutes   = round(fact_flights["duration_minutes"].mean(), 1)
overnight_flight_count = fact_flights["is_overnight"].sum()
overnight_flight_pct   = round((overnight_flight_count / total_flights) * 100, 1) if total_flights > 0 else 0.0

anomaly_flight_count   = fact_flights["is_anomaly"].sum()
anomaly_flight_pct     = round((anomaly_flight_count / total_flights) * 100, 1) if total_flights > 0 else 0.0

distinct_airline_count = len(dim_airline)
distinct_route_count   = len(dim_route)

snapshot_generated_at  = datetime.now(timezone.utc)

# Build DataFrame
summary_data = {
    "total_flights": [total_flights],
    "avg_duration_minutes": [avg_duration_minutes],
    "overnight_flight_count": [overnight_flight_count],
    "overnight_flight_pct": [overnight_flight_pct],
    "anomaly_flight_count": [anomaly_flight_count],
    "anomaly_flight_pct": [anomaly_flight_pct],
    "distinct_airline_count": [distinct_airline_count],
    "distinct_route_count": [distinct_route_count],
    "snapshot_generated_at": [snapshot_generated_at]
}

df_summary = pd.DataFrame(summary_data)

# Write to ADLS
print(f"[3/3] Writing to az://{CONTAINER_GOLD}/gold_kpi_summary...")
write_deltalake(f"az://{CONTAINER_GOLD}/gold_kpi_summary", df_summary, mode="overwrite", schema_mode="overwrite", storage_options=so)

# Print Validation Report
print("\n" + "=" * 80)
print("TASK 7b VALIDATION REPORT")
print("=" * 80)

print("\n1. SINGLE ROW OUTPUT DUMP:")
for col in df_summary.columns:
    print(f"   {col:<25} : {df_summary.iloc[0][col]}")

print("\n2. SANITY CHECK: OVERNIGHT FLIGHTS")
non_overnight = total_flights - overnight_flight_count
print(f"   Overnight: {overnight_flight_count} + Non-Overnight: {non_overnight} = {overnight_flight_count + non_overnight} (Expected: {total_flights})")
if (overnight_flight_count + non_overnight) == total_flights:
    print("   -> PASS: Counts add up exactly.")
else:
    print("   -> FAIL: Counts mismatch.")

print("\n3. SANITY CHECK: ANOMALIES (<= TOTAL)")
print(f"   Total flights  : {total_flights}")
print(f"   Anomaly flights: {anomaly_flight_count} ({anomaly_flight_pct}%)")

anomaly_valid = anomaly_flight_count <= total_flights

if anomaly_valid:
    print("   -> PASS: Anomaly counts are within valid range.")
else:
    print("   -> FAIL: Anomaly count exceeds total.")

print("\nGold KPI Summary generation complete.")
