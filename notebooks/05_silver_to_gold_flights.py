"""
ASG Airlines — Silver to Gold Layer Pipeline
=================================================
Target Tables:  ADLS Gen2 'gold/dim_airline', 'gold/dim_route', 'gold/dim_date', 'gold/fact_flights'

Builds the star schema dimensional model for flights.
"""

import os
import pandas as pd
from deltalake import write_deltalake, DeltaTable

STORAGE_ACCOUNT_NAME = "stasgairlines01"
CONTAINER_SILVER     = "silver"
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
print("ASG AIRLINES — TASK 7a: GOLD FLIGHTS STAR SCHEMA")
print("=" * 80)

print("\n[1/5] Reading silver/flights...")
df_flights = DeltaTable(f"az://{CONTAINER_SILVER}/flights", storage_options=so).to_pandas()
print(f"      Loaded: {len(df_flights):,} rows")

# ---------------------------------------------------------
# Build dim_airline
# ---------------------------------------------------------
print("\n[2/5] Building dim_airline...")
dim_airline = df_flights[["airline"]].drop_duplicates().reset_index(drop=True)
dim_airline.rename(columns={"airline": "airline_name"}, inplace=True)
dim_airline.insert(0, "airline_id", range(1, len(dim_airline) + 1))
print(f"      Rows: {len(dim_airline)}")

# ---------------------------------------------------------
# Build dim_route
# ---------------------------------------------------------
print("\n[3/5] Building dim_route...")
dim_route = df_flights[["source", "destination"]].drop_duplicates().reset_index(drop=True)
dim_route.insert(0, "route_id", range(1, len(dim_route) + 1))
print(f"      Rows: {len(dim_route)}")

# ---------------------------------------------------------
# Build dim_date
# ---------------------------------------------------------
print("\n[4/5] Building dim_date...")
dates = pd.to_datetime(df_flights["departure_time"], format='mixed').dt.date.drop_duplicates().reset_index(drop=True)
dim_date = pd.DataFrame({"full_date": dates})
# date_id as YYYYMMDD
dim_date["date_id"] = pd.to_datetime(dim_date["full_date"]).dt.strftime("%Y%m%d").astype(int)
dim_date["day_of_week"] = pd.to_datetime(dim_date["full_date"]).dt.day_name()
dim_date["month"] = pd.to_datetime(dim_date["full_date"]).dt.month_name()
dim_date["is_weekend"] = pd.to_datetime(dim_date["full_date"]).dt.dayofweek.isin([5, 6])
# Reorder
dim_date = dim_date[["date_id", "full_date", "day_of_week", "month", "is_weekend"]]
print(f"      Rows: {len(dim_date)}")

# ---------------------------------------------------------
# Build fact_flights
# ---------------------------------------------------------
print("\n[5/5] Building fact_flights...")
fact_flights = df_flights.copy()

# Join dim_airline
fact_flights = fact_flights.merge(
    dim_airline, 
    left_on="airline", 
    right_on="airline_name", 
    how="left"
)

# Join dim_route
fact_flights = fact_flights.merge(
    dim_route, 
    on=["source", "destination"], 
    how="left"
)

# Join dim_date
fact_flights["dep_date"] = pd.to_datetime(fact_flights["departure_time"], format='mixed').dt.date
fact_flights = fact_flights.merge(
    dim_date[["date_id", "full_date"]], 
    left_on="dep_date", 
    right_on="full_date", 
    how="left"
)

# Check for orphans
missing_airlines = fact_flights["airline_id"].isna().sum()
missing_routes   = fact_flights["route_id"].isna().sum()
missing_dates    = fact_flights["date_id"].isna().sum()

# Transformations
fact_flights.rename(columns={
    "flight_id": "flight_id_original",
    "departure_time": "departure_datetime",
    "arrival_time": "arrival_datetime",
    "is_overnight_flight": "is_overnight",
    "duration_is_anomalous": "is_anomaly"
}, inplace=True)

fact_flights.insert(0, "flight_id", range(1, len(fact_flights) + 1))

cols_fact = [
    "flight_id", "flight_id_original", "airline_id", "route_id", "date_id",
    "departure_datetime", "arrival_datetime", "duration_minutes",
    "is_overnight", "is_anomaly"
]
fact_flights = fact_flights[cols_fact]

# Write out to ADLS
print("\nWriting tables to ADLS Gen2...")
write_deltalake(f"az://{CONTAINER_GOLD}/dim_airline", dim_airline, mode="overwrite", schema_mode="overwrite", storage_options=so)
write_deltalake(f"az://{CONTAINER_GOLD}/dim_route", dim_route, mode="overwrite", schema_mode="overwrite", storage_options=so)
write_deltalake(f"az://{CONTAINER_GOLD}/dim_date", dim_date, mode="overwrite", schema_mode="overwrite", storage_options=so)
write_deltalake(f"az://{CONTAINER_GOLD}/fact_flights", fact_flights, mode="overwrite", schema_mode="overwrite", storage_options=so)

print("\n" + "=" * 80)
print("TASK 7a VALIDATION REPORT")
print("=" * 80)

print("\n1. ROW COUNTS & FAN-OUT CHECK")
print(f"   silver/flights : {len(df_flights)} rows")
print(f"   gold/fact_flights: {len(fact_flights)} rows")
if len(df_flights) == len(fact_flights):
    print("   -> PASS (Exact match, no fan-out or row loss)")
else:
    print("   -> FAIL (Row counts do not match)")

print("\n2. ORPHANED FOREIGN KEYS CHECK")
print(f"   airline_id NULLs: {missing_airlines}")
print(f"   route_id NULLs  : {missing_routes}")
print(f"   date_id NULLs   : {missing_dates}")
if missing_airlines == 0 and missing_routes == 0 and missing_dates == 0:
    print("   -> PASS (All facts successfully joined to dimensions)")
else:
    print("   -> FAIL (Some records missed joins)")

print("\n3. DIMENSION CARDINALITIES")
print(f"   dim_airline: {len(dim_airline)} distinct airlines")
print(f"   dim_route  : {len(dim_route)} distinct routes")
print(f"   dim_date   : {len(dim_date)} distinct dates")

print("\n4. GOLD SCHEMA DUMP (checking for passenger/booking leakage)")
print(f"   dim_airline: {list(dim_airline.columns)}")
print(f"   dim_route  : {list(dim_route.columns)}")
print(f"   dim_date   : {list(dim_date.columns)}")
print(f"   fact_flights: {list(fact_flights.columns)}")

print("\n5. FACT SAMPLE (3 rows, visually joined for display)")
display_sample = fact_flights.head(3).copy()
# Re-join visually
display_sample = display_sample.merge(dim_airline, on="airline_id", how="left")
display_sample = display_sample.merge(dim_route, on="route_id", how="left")
display_sample = display_sample.merge(dim_date, on="date_id", how="left")
for _, row in display_sample.iterrows():
    print(f"   Fact ID {row['flight_id']} ({row['flight_id_original']}): "
          f"Airline '{row['airline_name']}' | "
          f"Route {row['source']}->{row['destination']} | "
          f"Date {row['date_id']} | "
          f"Duration {row['duration_minutes']}m | "
          f"Overnight: {row['is_overnight']} | Anomaly: {row['is_anomaly']}")

print("\nGold layer generation complete.")
