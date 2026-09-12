"""
ASG Airlines — Bronze to Silver Transformation Pipeline
=======================================================
Execution Mode: Native Python + Delta Lake (deltalake / delta-rs) & ADLS Gen2
Storage Layer:  Read from ADLS Gen2 'bronze' container -> Write to ADLS Gen2 'silver' container

Rules Applied:
  1. flights: Drop 15 fully-identical duplicate rows (keep 1 each).
  2. flights: Quarantine rows with conflicting flight_id details (e.g. 6F250, 2 rows)
     to silver/rejected_flights_id_conflicts with rejection_reason.
  3. flights: Drop raw `duration` column (will be recomputed in Task 5).
  4. flights: Standardize `airline` (41 nulls set to "Unknown", flag `airline_is_missing = True`).
  5. payments: Cast `amount` to numeric float (30 'INVALID' + 48 nulls set to NaN/None,
     flag `amount_is_invalid = True`).
  6. bookings: Standardize `status` (45 nulls set to "Unknown", flag `status_is_missing = True`).
  7. passengers: Standardize `last_name` (10 nulls set to "Unknown", flag `last_name_is_missing = True`).
"""

import os
import datetime
import pandas as pd
import numpy as np
from deltalake import write_deltalake, DeltaTable

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Storage Configuration & Connection Setup
# ─────────────────────────────────────────────────────────────────────────────

STORAGE_ACCOUNT_NAME = "stasgairlines01"
CONTAINER_BRONZE     = "bronze"
CONTAINER_SILVER     = "silver"

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
    raise RuntimeError(
        "ADLS_STORAGE_KEY environment variable is not set and az CLI lookup failed."
    )

storage_options = {
    "azure_storage_account_name": STORAGE_ACCOUNT_NAME,
    "azure_storage_access_key": STORAGE_KEY,
}

print("=" * 80)
print("ASG AIRLINES — BRONZE TO SILVER TRANSFORMATION PIPELINE")
print(f"Storage Account: {STORAGE_ACCOUNT_NAME}")
print("=" * 80)

# Read Bronze Delta Tables
print("\n[1/5] Reading Bronze Delta tables from ADLS Gen2...")

df_flights_bronze    = DeltaTable(f"az://{CONTAINER_BRONZE}/flights", storage_options=storage_options).to_pandas()
df_bookings_bronze   = DeltaTable(f"az://{CONTAINER_BRONZE}/bookings", storage_options=storage_options).to_pandas()
df_passengers_bronze = DeltaTable(f"az://{CONTAINER_BRONZE}/passengers", storage_options=storage_options).to_pandas()
df_payments_bronze   = DeltaTable(f"az://{CONTAINER_BRONZE}/payments", storage_options=storage_options).to_pandas()

print(f"      Loaded Bronze 'flights'   : {len(df_flights_bronze):,} rows")
print(f"      Loaded Bronze 'bookings'  : {len(df_bookings_bronze):,} rows")
print(f"      Loaded Bronze 'passengers': {len(df_passengers_bronze):,} rows")
print(f"      Loaded Bronze 'payments'  : {len(df_payments_bronze):,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Apply Silver Cleaning & Standardization Rules
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/5] Applying Silver transformations and data quality rules...")

summary_report = []

# -----------------------------------------------------------------------------
# RULE 1, 2, 3, 4: FLIGHTS TABLE TRANSFORMATIONS
# -----------------------------------------------------------------------------
df_flights = df_flights_bronze.copy()
initial_flights_cnt = len(df_flights)

# Rule 1: Drop fully identical duplicate rows
source_cols_flights = ['flight_id', 'airline', 'source', 'destination', 'departure_time', 'arrival_time', 'duration']
distinct_flights_df = df_flights.drop_duplicates(subset=source_cols_flights, keep='first')
dedup_dropped_cnt = len(df_flights) - len(distinct_flights_df)
df_flights = distinct_flights_df.copy()

summary_report.append({
    "Table": "flights",
    "Rule #": 1,
    "Rule Description": "Drop fully-identical duplicate rows",
    "Affected Count": dedup_dropped_cnt,
    "Action Taken": f"Dropped {dedup_dropped_cnt} identical duplicate rows (kept 1 each)"
})

# Rule 2: Quarantine conflicting flight_id rows (flight_ids appearing 2+ times with differing fields)
flight_id_counts = df_flights['flight_id'].value_counts()
potential_conflicts = flight_id_counts[flight_id_counts > 1].index

conflicting_flight_ids = []
for fid in potential_conflicts:
    sub = df_flights[df_flights['flight_id'] == fid][source_cols_flights]
    if len(sub.drop_duplicates()) > 1:
        conflicting_flight_ids.append(fid)

quarantine_mask = df_flights['flight_id'].isin(conflicting_flight_ids)
df_quarantine = df_flights[quarantine_mask].copy()
df_flights = df_flights[~quarantine_mask].copy()

# Add rejection metadata to quarantine table
df_quarantine['rejection_reason'] = "Conflicting flight_id details across rows"
df_quarantine['quarantine_timestamp'] = datetime.datetime.now(datetime.timezone.utc)

quarantine_cnt = len(df_quarantine)
summary_report.append({
    "Table": "flights",
    "Rule #": 2,
    "Rule Description": "Quarantine conflicting flight_id records",
    "Affected Count": quarantine_cnt,
    "Action Taken": f"Quarantined {quarantine_cnt} rows (flight_ids: {conflicting_flight_ids}) to silver/rejected_flights_id_conflicts"
})

# Rule 3: Drop raw `duration` column
if 'duration' in df_flights.columns:
    df_flights = df_flights.drop(columns=['duration'])

summary_report.append({
    "Table": "flights",
    "Rule #": 3,
    "Rule Description": "Drop raw duration column",
    "Affected Count": initial_flights_cnt,
    "Action Taken": "Dropped raw 'duration' column (to be recomputed in Task 5)"
})

# Rule 4: Standardize `airline` (41 nulls -> "Unknown", flag airline_is_missing = True)
airline_null_mask = df_flights['airline'].isna() | (df_flights['airline'].astype(str).str.strip() == '') | (df_flights['airline'] == 'UNKNOWN') | (df_flights['airline'] == 'Unknown')
# Note: In raw data, missing airlines were 'UNKNOWN' or None/NaN
airline_missing_cnt = airline_null_mask.sum()

df_flights['airline_is_missing'] = airline_null_mask
df_flights.loc[airline_null_mask, 'airline'] = "Unknown"

summary_report.append({
    "Table": "flights",
    "Rule #": 4,
    "Rule Description": "Standardize missing airline",
    "Affected Count": int(airline_missing_cnt),
    "Action Taken": f"Set {airline_missing_cnt} null/UNKNOWN airlines to 'Unknown', flag airline_is_missing = True"
})

# -----------------------------------------------------------------------------
# RULE 5: PAYMENTS TABLE TRANSFORMATIONS
# -----------------------------------------------------------------------------
df_payments = df_payments_bronze.copy()

# Rule 5: Cast `amount` to numeric float; INVALID or null -> None, amount_is_invalid = True
numeric_amounts = pd.to_numeric(df_payments['amount'], errors='coerce')
amount_invalid_mask = numeric_amounts.isna()

amount_invalid_cnt = int(amount_invalid_mask.sum())

df_payments['amount_is_invalid'] = amount_invalid_mask
df_payments['amount'] = numeric_amounts

summary_report.append({
    "Table": "payments",
    "Rule #": 5,
    "Rule Description": "Cast amount to numeric & flag invalid/nulls",
    "Affected Count": amount_invalid_cnt,
    "Action Taken": f"Cast amount to float. Set {amount_invalid_cnt} invalid/null amounts to null, flag amount_is_invalid = True"
})

# -----------------------------------------------------------------------------
# RULE 6: BOOKINGS TABLE TRANSFORMATIONS
# -----------------------------------------------------------------------------
df_bookings = df_bookings_bronze.copy()

# Rule 6: Standardize `status` (45 nulls -> "Unknown", flag status_is_missing = True)
status_null_mask = df_bookings['status'].isna() | (df_bookings['status'].astype(str).str.strip() == '')
status_missing_cnt = int(status_null_mask.sum())

df_bookings['status_is_missing'] = status_null_mask
df_bookings.loc[status_null_mask, 'status'] = "Unknown"

summary_report.append({
    "Table": "bookings",
    "Rule #": 6,
    "Rule Description": "Standardize missing booking status",
    "Affected Count": status_missing_cnt,
    "Action Taken": f"Set {status_missing_cnt} null status values to 'Unknown', flag status_is_missing = True"
})

# -----------------------------------------------------------------------------
# RULE 7: PASSENGERS TABLE TRANSFORMATIONS
# -----------------------------------------------------------------------------
df_passengers = df_passengers_bronze.copy()

# Rule 7: Standardize `last_name` (10 nulls -> "Unknown", flag last_name_is_missing = True)
lname_null_mask = df_passengers['last_name'].isna() | (df_passengers['last_name'].astype(str).str.strip() == '')
lname_missing_cnt = int(lname_null_mask.sum())

df_passengers['last_name_is_missing'] = lname_null_mask
df_passengers.loc[lname_null_mask, 'last_name'] = "Unknown"

summary_report.append({
    "Table": "passengers",
    "Rule #": 7,
    "Rule Description": "Standardize missing last_name",
    "Affected Count": lname_missing_cnt,
    "Action Taken": f"Set {lname_missing_cnt} null last_names to 'Unknown', flag last_name_is_missing = True"
})

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Print Cleaning Summary Report
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 80)
print("ASG AIRLINES — SILVER CLEANING & STANDARDIZATION REPORT")
print("=" * 80)

summary_df = pd.DataFrame(summary_report)
for idx, row in summary_df.iterrows():
    print(f"\n[Rule {row['Rule #']}] {row['Table'].upper()}: {row['Rule Description']}")
    print(f"         Affected Rows : {row['Affected Count']}")
    print(f"         Action Taken  : {row['Action Taken']}")

print("\n" + "-" * 80)
print("BEFORE vs AFTER ROW COUNTS SUMMARY:")
print("-" * 80)
print(f"  flights    : Bronze = {len(df_flights_bronze):>5} -> Silver = {len(df_flights):>5}  (Dropped {dedup_dropped_cnt} dups, Quarantined {quarantine_cnt})")
print(f"  bookings   : Bronze = {len(df_bookings_bronze):>5} -> Silver = {len(df_bookings):>5}")
print(f"  passengers : Bronze = {len(df_passengers_bronze):>5} -> Silver = {len(df_passengers):>5}")
print(f"  payments   : Bronze = {len(df_payments_bronze):>5} -> Silver = {len(df_payments):>5}")
print(f"  quarantine : Rejected Flights ID Conflicts = {len(df_quarantine):>5} rows")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Write Cleaned Silver Delta Tables to ADLS Gen2 'silver' container
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[3/5] Writing Silver Delta Lake tables to ADLS Gen2 '{CONTAINER_SILVER}' container...")

silver_tables = {
    "flights": df_flights,
    "bookings": df_bookings,
    "passengers": df_passengers,
    "payments": df_payments,
    "rejected_flights_id_conflicts": df_quarantine
}

for name, df in silver_tables.items():
    silver_uri = f"az://{CONTAINER_SILVER}/{name}"
    print(f"      Writing '{name}' -> {silver_uri} ...")
    write_deltalake(silver_uri, df, mode="overwrite", schema_mode="overwrite", storage_options=storage_options)
    print(f"      [OK] Wrote {len(df):,} rows to {silver_uri}")

# Also save local copy of quarantine table to data/rejected/flights_id_conflicts
local_quarantine_dir = os.path.join("data", "rejected")
os.makedirs(local_quarantine_dir, exist_ok=True)
local_quarantine_path = os.path.join(local_quarantine_dir, "flights_id_conflicts.parquet")
df_quarantine.to_parquet(local_quarantine_path, index=False)
print(f"      [OK] Saved local copy of quarantine records -> {local_quarantine_path}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Verification & Readback
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[4/5] Verifying Silver Delta tables in ADLS Gen2...")

for name in silver_tables.keys():
    silver_uri = f"az://{CONTAINER_SILVER}/{name}"
    dt = DeltaTable(silver_uri, storage_options=storage_options)
    v_df = dt.to_pandas()
    print(f"      [Verified] silver/{name:<30}: {len(v_df):,} rows | Schema fields: {len(dt.schema().fields)}")

print("\n[5/5] Transformation to Silver Layer COMPLETE!")
