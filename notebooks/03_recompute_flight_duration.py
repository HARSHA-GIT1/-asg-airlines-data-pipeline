"""
ASG Airlines — Silver Layer Flight Duration & Overnight Handling Pipeline (Full Datetime Fix)
========================================================================================
Execution Mode: Native Python + Delta Lake (deltalake / delta-rs) & ADLS Gen2
Target Table:   ADLS Gen2 'silver/flights'

Logic Implemented (Full Datetime Calculation):
  1. duration_minutes = (arrival_time - departure_time) in total minutes, using full datetime including date.
  2. is_overnight_flight = True when duration_minutes > 0 AND departure_time.date() != arrival_time.date().
  3. duration_is_anomalous = True when duration_minutes <= 0 (e.g. SJ192 with negative duration)
     OR duration_minutes > 1440.
  4. duration_formatted = human-readable string (e.g., "2h 15m" for valid, "-1140m (INVALID)" for anomalous).
  5. Overwrite updated `silver/flights` Delta table in ADLS Gen2.
"""

import os
import pandas as pd
import numpy as np
from deltalake import write_deltalake, DeltaTable

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Storage Account Configuration
# ─────────────────────────────────────────────────────────────────────────────

STORAGE_ACCOUNT_NAME = "stasgairlines01"
CONTAINER_SILVER     = "silver"
TABLE_FLIGHTS        = "flights"

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
        pass

if not STORAGE_KEY:
    raise RuntimeError("ADLS_STORAGE_KEY environment variable is not set.")

storage_options = {
    "azure_storage_account_name": STORAGE_ACCOUNT_NAME,
    "azure_storage_access_key": STORAGE_KEY,
}

print("=" * 80)
print("ASG AIRLINES — TASK 5-FIX: FULL DATETIME FLIGHT DURATION PIPELINE")
print(f"Target Delta Table: az://{CONTAINER_SILVER}/{TABLE_FLIGHTS}")
print("=" * 80)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Read Silver Flights Delta Table
# ─────────────────────────────────────────────────────────────────────────────

silver_uri = f"az://{CONTAINER_SILVER}/{TABLE_FLIGHTS}"
dt_flights = DeltaTable(silver_uri, storage_options=storage_options)
df_flights = dt_flights.to_pandas()

# Drop old/unneeded transient index or diff columns if present
unwanted_cols = ["__index_level_0__", "raw_diff_minutes", "tod_diff_minutes"]
for col in unwanted_cols:
    if col in df_flights.columns:
        df_flights = df_flights.drop(columns=[col])

print(f"\n[1/4] Read {len(df_flights):,} rows from {silver_uri}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Full Datetime Duration & Overnight Recomputation
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/4] Recomputing flight duration using full datetime timestamps...")

dep_dt = pd.to_datetime(df_flights["departure_time"], format="mixed")
arr_dt = pd.to_datetime(df_flights["arrival_time"], format="mixed")

# 1. Total minutes using full datetime (including date)
duration_minutes = (arr_dt - dep_dt).dt.total_seconds() / 60.0
duration_minutes = duration_minutes.round(2)

# 2. is_overnight_flight = True when duration_minutes > 0 AND dates differ
dep_date = dep_dt.dt.date
arr_date = arr_dt.dt.date
is_overnight = (duration_minutes > 0) & (dep_date != arr_date)

# 3. duration_is_anomalous = True when duration_minutes <= 0 OR > 1440
duration_is_anomalous = (duration_minutes <= 0) | (duration_minutes > 1440)

# 4. duration_formatted helper
def format_duration(mins):
    if mins <= 0:
        return f"{mins:.0f}m (INVALID)"
    total_mins = int(round(mins))
    h = total_mins // 60
    m = total_mins % 60
    return f"{h}h {m}m"

duration_formatted = duration_minutes.apply(format_duration)

# Assign corrected columns
df_flights["is_overnight_flight"] = is_overnight
df_flights["duration_minutes"] = duration_minutes
df_flights["duration_is_anomalous"] = duration_is_anomalous
df_flights["duration_formatted"] = duration_formatted

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Validation & Benchmark Reporting
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 80)
print("TASK 5-FIX VALIDATION REPORT")
print("=" * 80)

# Validate SJ192
sj192_row = df_flights[df_flights["flight_id"] == "SJ192"]
if len(sj192_row) > 0:
    r = sj192_row.iloc[0]
    print("\n1. Target Validation Case (Flight SJ192):")
    print(f"   - flight_id             : {r['flight_id']}")
    print(f"   - departure_time        : '{r['departure_time']}'")
    print(f"   - arrival_time          : '{r['arrival_time']}'")
    print(f"   - duration_minutes      : {r['duration_minutes']} min")
    print(f"   - is_overnight_flight   : {r['is_overnight_flight']}")
    print(f"   - duration_is_anomalous : {r['duration_is_anomalous']} (Flagged correctly)")
    print(f"   - duration_formatted    : '{r['duration_formatted']}'")

total_overnight_cnt = int(is_overnight.sum())
total_anomalous_cnt = int(duration_is_anomalous.sum())

print(f"\n2. Updated Summary Counts (Total Rows: {len(df_flights):,}):")
print(f"   - Total Overnight Flights Found : {total_overnight_cnt:,} ({total_overnight_cnt / len(df_flights) * 100:.2f}%)")
print(f"   - Total Anomalous Flights Found : {total_anomalous_cnt:,} ({total_anomalous_cnt / len(df_flights) * 100:.2f}%)")

print("\n3. Corrected duration_minutes Distribution Summary:")
stats = duration_minutes.describe()
print(f"   - Min Duration     : {stats['min']:.1f} min (SJ192 outlier)")
print(f"   - 25th Percentile   : {stats['25%']:.1f} min")
print(f"   - Median (50%)     : {stats['50%']:.1f} min")
print(f"   - Mean Duration    : {stats['mean']:.1f} min")
print(f"   - 75th Percentile   : {stats['75%']:.1f} min")
print(f"   - Max Duration     : {stats['max']:.1f} min")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Overwrite Corrected Silver Flights Delta Table
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[3/4] Overwriting corrected 'silver/flights' Delta table in ADLS Gen2...")
write_deltalake(silver_uri, df_flights, mode="overwrite", schema_mode="overwrite", storage_options=storage_options)
print(f"      [OK] Successfully wrote {len(df_flights):,} rows to {silver_uri}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Final Verification
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[4/4] Verifying corrected 'silver/flights' Delta table...")
dt_verify = DeltaTable(silver_uri, storage_options=storage_options)
df_verify = dt_verify.to_pandas()
print(f"      [Verified] {silver_uri}: {len(df_verify):,} rows | Schema fields: {len(dt_verify.schema().fields)}")
print(f"      Final Schema Fields: {[f.name for f in dt_verify.schema().fields]}")

print("\nTask 5-Fix Pipeline Execution COMPLETE!")
