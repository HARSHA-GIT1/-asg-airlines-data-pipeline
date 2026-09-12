"""
ASG Airlines — Silver Layer PII Masking Pipeline
=================================================
Execution Mode: Native Python + Delta Lake (deltalake / delta-rs) & ADLS Gen2
Target Tables:  ADLS Gen2 'silver/passengers' and 'silver/bookings'

Masking rules applied (Silver):
  passengers:
    - aadhaar_id      -> aadhaar_id_hash (SHA-256), original DROPPED
    - email           -> email_masked (partial), original DROPPED
    - phone           -> phone_masked (partial), original DROPPED
    - first_name, last_name, date_of_birth: UNCHANGED (needed for Gold joins)

  bookings:
    - passport_number         -> passport_number_hash (SHA-256), original DROPPED
    - emergency_contact_name  -> emergency_contact_name_masked, original DROPPED
    - emergency_contact_phone -> emergency_contact_phone_masked, original DROPPED
"""

import os
import sys
import pandas as pd
from deltalake import write_deltalake, DeltaTable

# Add project root to path so we can import utils/pii_masking
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.pii_masking import hash_pii, mask_email, mask_phone, mask_name

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Load Salt & Storage Configuration
# ─────────────────────────────────────────────────────────────────────────────

STORAGE_ACCOUNT_NAME = "stasgairlines01"
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
    raise RuntimeError("ADLS_STORAGE_KEY not set.")

# Load PII_SALT from environment (set externally from .env, never hardcoded)
PII_SALT = os.environ.get("PII_SALT", "")
if not PII_SALT:
    # Attempt to load from .env file (dev convenience only — in prod, set via env)
    env_file = os.path.join(PROJECT_ROOT, ".env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("PII_SALT="):
                    PII_SALT = line.split("=", 1)[1].strip()
                    break

if not PII_SALT:
    raise RuntimeError(
        "PII_SALT environment variable is not set. "
        "Set it from .env: $env:PII_SALT = (Get-Content .env | Select-String 'PII_SALT').ToString().Split('=')[1]"
    )

storage_options = {
    "azure_storage_account_name": STORAGE_ACCOUNT_NAME,
    "azure_storage_access_key": STORAGE_KEY,
}

print("=" * 80)
print("ASG AIRLINES — TASK 6: SILVER PII MASKING PIPELINE")
print(f"Storage Account : {STORAGE_ACCOUNT_NAME}")
print(f"PII_SALT loaded : {'[REDACTED — ' + str(len(PII_SALT)) + ' chars]'}")
print("=" * 80)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Read Silver Delta Tables
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/5] Reading Silver Delta tables from ADLS Gen2...")

df_passengers = DeltaTable(f"az://{CONTAINER_SILVER}/passengers", storage_options=storage_options).to_pandas()
df_bookings   = DeltaTable(f"az://{CONTAINER_SILVER}/bookings",   storage_options=storage_options).to_pandas()

print(f"      Loaded Silver 'passengers': {len(df_passengers):,} rows")
print(f"      Loaded Silver 'bookings'  : {len(df_bookings):,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Apply PII Masking — PASSENGERS
# ─────────────────────────────────────────────────────────────────────────────

print("\n[2/5] Applying PII masking to 'passengers' table...")

# Rule: aadhaar_id -> SHA-256 hash, drop original
df_passengers["aadhaar_id_hash"] = df_passengers["aadhaar_id"].apply(lambda v: hash_pii(PII_SALT, v))
df_passengers = df_passengers.drop(columns=["aadhaar_id"])

# Rule: email -> partial mask, drop original
df_passengers["email_masked"] = df_passengers["email"].apply(mask_email)
df_passengers = df_passengers.drop(columns=["email"])

# Rule: phone -> partial mask, drop original
df_passengers["phone_masked"] = df_passengers["phone"].apply(mask_phone)
df_passengers = df_passengers.drop(columns=["phone"])

print("      Masked: aadhaar_id -> aadhaar_id_hash (SHA-256, dropped original)")
print("      Masked: email -> email_masked (partial, dropped original)")
print("      Masked: phone -> phone_masked (partial, dropped original)")
print("      Retained unchanged: first_name, last_name, date_of_birth, age, gender")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Apply PII Masking — BOOKINGS
# ─────────────────────────────────────────────────────────────────────────────

print("\n[3/5] Applying PII masking to 'bookings' table...")

# Rule: passport_number -> SHA-256 hash, drop original
df_bookings["passport_number_hash"] = df_bookings["passport_number"].apply(lambda v: hash_pii(PII_SALT, v))
df_bookings = df_bookings.drop(columns=["passport_number"])

# Rule: emergency_contact_name -> first char + "***", drop original
df_bookings["emergency_contact_name_masked"] = df_bookings["emergency_contact_name"].apply(mask_name)
df_bookings = df_bookings.drop(columns=["emergency_contact_name"])

# Rule: emergency_contact_phone -> partial mask, drop original
df_bookings["emergency_contact_phone_masked"] = df_bookings["emergency_contact_phone"].apply(mask_phone)
df_bookings = df_bookings.drop(columns=["emergency_contact_phone"])

print("      Masked: passport_number -> passport_number_hash (SHA-256, dropped original)")
print("      Masked: emergency_contact_name -> emergency_contact_name_masked (dropped original)")
print("      Masked: emergency_contact_phone -> emergency_contact_phone_masked (dropped original)")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Validation Report
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 80)
print("TASK 6 PII MASKING VALIDATION REPORT")
print("=" * 80)

BANNED_PASSENGERS = {"aadhaar_id", "email", "phone"}
BANNED_BOOKINGS   = {"passport_number", "emergency_contact_name", "emergency_contact_phone"}

passengers_cols = set(df_passengers.columns)
bookings_cols   = set(df_bookings.columns)

print("\n1. PASSENGERS: Raw PII columns removed?")
for col in sorted(BANNED_PASSENGERS):
    still_present = col in passengers_cols
    status = "FAIL — STILL PRESENT" if still_present else "PASS — removed"
    print(f"   - {col:<35}: {status}")

print("\n2. BOOKINGS: Raw PII columns removed?")
for col in sorted(BANNED_BOOKINGS):
    still_present = col in bookings_cols
    status = "FAIL — STILL PRESENT" if still_present else "PASS — removed"
    print(f"   - {col:<35}: {status}")

print("\n3. Masked column samples (passengers):")
sample_p = df_passengers[["passenger_id", "aadhaar_id_hash", "email_masked", "phone_masked"]].head(3)
for _, row in sample_p.iterrows():
    print(f"   {row['passenger_id']}: aadhaar_hash={row['aadhaar_id_hash'][:16]}... | "
          f"email={row['email_masked']} | phone={row['phone_masked']}")

print("\n4. Masked column samples (bookings):")
sample_b = df_bookings[["booking_id", "passport_number_hash", "emergency_contact_name_masked", "emergency_contact_phone_masked"]].head(3)
for _, row in sample_b.iterrows():
    print(f"   {row['booking_id']}: passport_hash={row['passport_number_hash'][:16]}... | "
          f"name={row['emergency_contact_name_masked']} | phone={row['emergency_contact_phone_masked']}")

print("\n5. Confirmed retained (unchanged in Silver):")
retained_p = ["first_name", "last_name", "date_of_birth"]
for col in retained_p:
    print(f"   passengers.{col}: {'PRESENT' if col in passengers_cols else 'MISSING — ERROR'}")

print(f"\n6. Final Silver Schema Counts:")
print(f"   passengers: {len(df_passengers)} rows | {len(df_passengers.columns)} columns")
print(f"   bookings  : {len(df_bookings)} rows | {len(df_bookings.columns)} columns")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Overwrite Silver Delta Tables
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[4/5] Overwriting masked Silver Delta tables in ADLS Gen2...")

write_deltalake(
    f"az://{CONTAINER_SILVER}/passengers",
    df_passengers, mode="overwrite", schema_mode="overwrite",
    storage_options=storage_options
)
print(f"      [OK] Wrote {len(df_passengers):,} rows -> az://{CONTAINER_SILVER}/passengers")

write_deltalake(
    f"az://{CONTAINER_SILVER}/bookings",
    df_bookings, mode="overwrite", schema_mode="overwrite",
    storage_options=storage_options
)
print(f"      [OK] Wrote {len(df_bookings):,} rows -> az://{CONTAINER_SILVER}/bookings")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: Readback & Final Verification
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n[5/5] Verifying masked Silver Delta tables in ADLS Gen2...")

for tbl in ["passengers", "bookings"]:
    uri = f"az://{CONTAINER_SILVER}/{tbl}"
    dt  = DeltaTable(uri, storage_options=storage_options)
    df  = dt.to_pandas()
    print(f"      [Verified] silver/{tbl}: {len(df):,} rows | Schema: {[f.name for f in dt.schema().fields]}")

print("\nTask 6 PII Masking Pipeline COMPLETE!")
