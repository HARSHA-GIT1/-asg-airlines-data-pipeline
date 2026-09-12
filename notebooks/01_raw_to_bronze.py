"""
ASG Airlines — Raw to Bronze Ingestion Pipeline
================================================
Execution Mode: Native Python + Delta Lake (deltalake / delta-rs) & Azure Data Lake Storage Gen2
Storage Layer:  Azure Data Lake Storage Gen2 (az:// protocol)

Steps:
  1. Download Excel workbook (UseCase_-_Airlines.xlsx) from ADLS Gen2 raw container via Azure SDK
  2. Parse all 4 sheets (flights, bookings, passengers, payments) into DataFrames
  3. Add ingestion metadata columns: ingestion_timestamp (UTC) and source_file
  4. Perform Ingestion-Time Data Quality Assessment:
     - Record counts per table
     - Schema & Data Types per table
     - Null / Missing count & percentage per column
     - Duplicate row count per table
  5. Write each sheet as an official Delta Lake table into ADLS Gen2 bronze container
  6. Read back and verify all 4 Bronze Delta tables
"""

import os
import io
import datetime
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import write_deltalake, DeltaTable

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Storage Account & Environment Configuration
# ─────────────────────────────────────────────────────────────────────────────

STORAGE_ACCOUNT_NAME = "stasgairlines01"
CONTAINER_RAW        = "raw"
CONTAINER_BRONZE     = "bronze"
RAW_FILE_NAME        = "UseCase_-_Airlines.xlsx"

STORAGE_KEY = os.environ.get("ADLS_STORAGE_KEY", "")
if not STORAGE_KEY:
    # Try getting key via az CLI if available
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
        "ADLS_STORAGE_KEY environment variable is not set and az CLI key lookup failed. "
        "Run: $env:ADLS_STORAGE_KEY = (az storage account keys list ...)"
    )

print("=" * 80)
print(f"ASG AIRLINES — RAW TO BRONZE INGESTION PIPELINE")
print(f"Storage Account: {STORAGE_ACCOUNT_NAME}")
print("=" * 80)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Download Excel workbook from ADLS Gen2 raw container
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[1/5] Downloading raw Excel from ADLS Gen2 'raw' container: {RAW_FILE_NAME}...")

service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=STORAGE_KEY
)
raw_file_client = service_client.get_file_system_client(CONTAINER_RAW).get_file_client(RAW_FILE_NAME)
download_stream = raw_file_client.download_file()
excel_bytes = download_stream.readall()

print(f"      Downloaded {len(excel_bytes):,} bytes successfully.")

excel_file = pd.ExcelFile(io.BytesIO(excel_bytes))
sheet_names = ["flights", "bookings", "passengers", "payments"]
print(f"      Workbook sheets found: {excel_file.sheet_names}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Load Data & Add Ingestion Metadata
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/5] Parsing sheets and attaching ingestion metadata...")

ingestion_time = datetime.datetime.now(datetime.timezone.utc)
raw_dfs = {}

for sheet in sheet_names:
    df = pd.read_excel(excel_file, sheet_name=sheet, dtype=str)
    
    # Clean object types and ensure string formatting/null representation is consistent
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        df[col] = df[col].replace({"nan": None, "None": None, "<NA>": None, "NaT": None, "": None})
    
    # Add audit columns
    df["ingestion_timestamp"] = ingestion_time
    df["source_file"] = RAW_FILE_NAME
    
    raw_dfs[sheet] = df
    print(f"      Loaded '{sheet}': {len(df):,} rows, {len(df.columns)-2} raw columns + 2 metadata columns.")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Ingestion-Time Data Quality Assessment
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 80)
print("ASG AIRLINES — INGESTION-TIME DATA QUALITY REPORT")
print("=" * 80)

for sheet, df in raw_dfs.items():
    total_rows = len(df)
    source_cols = [c for c in df.columns if c not in ["ingestion_timestamp", "source_file"]]
    
    print("\n" + "#" * 70)
    print(f"  TABLE: {sheet.upper()}  (Total Ingested Rows: {total_rows:,})")
    print("#" * 70)
    
    print("\n[1] Column Null / Missing Summary:")
    print(f"{'Column Name':<35} | {'Null Count':>10} | {'Null %':>8} | Data Type")
    print("-" * 72)
    for col in source_cols:
        null_cnt = df[col].isna().sum()
        null_pct = (null_cnt / total_rows * 100) if total_rows > 0 else 0
        dtype_str = str(df[col].dtype)
        print(f"{col:<35} | {null_cnt:>10,} | {null_pct:>7.2f}% | {dtype_str}")
        
    # Duplicate check on source columns
    distinct_cnt = len(df[source_cols].drop_duplicates())
    dup_cnt = total_rows - distinct_cnt
    dup_pct = (dup_cnt / total_rows * 100) if total_rows > 0 else 0
    
    print("\n[2] Duplicate Row Check (Source Columns):")
    print(f"    Distinct Rows  : {distinct_cnt:,}")
    print(f"    Duplicate Rows : {dup_cnt:,} ({dup_pct:.2f}%)")

print("\n" + "=" * 80)
print("END OF DATA QUALITY REPORT")
print("=" * 80)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Write Delta Tables to ADLS Gen2 Bronze Container
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[3/5] Writing Delta Lake tables to ADLS Gen2 'bronze' container...")

storage_options = {
    "azure_storage_account_name": STORAGE_ACCOUNT_NAME,
    "azure_storage_access_key": STORAGE_KEY,
}

for sheet, df in raw_dfs.items():
    delta_uri = f"az://{CONTAINER_BRONZE}/{sheet}"
    print(f"      Writing '{sheet}' -> {delta_uri} ...")
    write_deltalake(delta_uri, df, mode="overwrite", schema_mode="overwrite", storage_options=storage_options)
    print(f"      [OK] Wrote {len(df):,} rows to {delta_uri}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Verification & Readback
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[4/5] Verifying Bronze Delta tables in ADLS Gen2...")

for sheet in sheet_names:
    delta_uri = f"az://{CONTAINER_BRONZE}/{sheet}"
    dt = DeltaTable(delta_uri, storage_options=storage_options)
    verified_df = dt.to_pandas()
    print(f"      [Verified] bronze/{sheet}: {len(verified_df):,} rows | Schema fields: {len(dt.schema().fields)}")

print("\n[5/5] Ingestion to Bronze Layer COMPLETE!")
