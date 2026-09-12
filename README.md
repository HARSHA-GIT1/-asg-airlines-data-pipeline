# ASG Airlines — End-to-End Data Engineering Case Study

An end-to-end data engineering pipeline that ingests, profiles, cleans, standardizes, protects, and models ASG Airlines' operational data into an analytics-ready Gold layer for Power BI reporting.

---

## Problem Statement

ASG Airlines' operational data is distributed across multiple datasets covering:

- Flights
- Bookings
- Passengers
- Payments

The source data contains real-world data-quality challenges, including:

- Conflicting or colliding flight identifiers
- Exact duplicate flight records
- Inconsistent time and duration formats
- Missing values
- Aadhaar identifier formatting issues
- Overnight flights crossing calendar days
- Negative-duration anomalies
- Sensitive passenger and booking information

These issues can lead to incorrect flight-duration calculations, unreliable operational reporting, and unnecessary exposure of personally identifiable information (PII).

This project addresses these problems through a **Raw → Bronze → Silver → Gold Medallion Architecture**, with data-quality validation, flight-duration recomputation, PII protection, analytical modeling, KPI generation, and Power BI reporting.

---

# Architecture

```text
                    Excel Source Workbook
                 (Flights / Bookings /
                  Passengers / Payments)
                           │
                           ▼
                ┌───────────────────────┐
                │ Azure Data Lake       │
                │ Storage Gen2          │
                │                       │
                │ raw/                  │
                └───────────┬───────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Local Python Pipeline │
                │                       │
                │ pandas                │
                │ deltalake / delta-rs  │
                │ Azure Storage SDK     │
                └───────────┬───────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Bronze Layer          │
                │ Raw Delta Tables      │
                │ Profiling only        │
                └───────────┬───────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Silver Layer          │
                │ Cleaned + Validated   │
                │ PII Protected         │
                └───────────┬───────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Gold Layer            │
                │ Flight Star Schema    │
                │ KPI Summary           │
                └───────────┬───────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Databricks            │
                │ Unity Catalog         │
                │ External Delta Tables │
                └───────────┬───────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Power BI Desktop      │
                │ 5-Page Dashboard      │
                └───────────────────────┘
```

---

# Technology Decision

The original implementation plan targeted:

- Azure Databricks with PySpark for distributed processing
- Azure Data Factory for pipeline orchestration

During implementation, Databricks cluster provisioning failed repeatedly across three Azure regions because of Azure capacity stockouts and worker-environment issues.

Rather than claiming an infrastructure deployment that was not successfully completed, the processing layer was implemented using:

- Python
- pandas
- NumPy
- `deltalake` / delta-rs
- Azure Data Lake Storage SDK

The pipeline reads and writes Delta tables directly to Azure Data Lake Storage Gen2.

The Medallion Architecture and ADLS Gen2 storage design remain unchanged.

Databricks is used downstream through **Unity Catalog external Delta tables**, providing the serving/catalog layer consumed by Power BI.

The provisioning issue and design decision are documented in:

```text
docs/ASG_Airlines_Documentation.docx
docs/azure_setup.md
```

---

# Tech Stack

| Layer | Technology |
|---|---|
| Source | Microsoft Excel |
| Cloud Storage | Azure Data Lake Storage Gen2 |
| Processing | Python |
| Data Processing | pandas, NumPy |
| Table Format | Delta Lake |
| Delta Engine | `deltalake` / delta-rs |
| Azure Connectivity | Azure Data Lake Storage SDK |
| Data Catalog / Serving | Databricks Unity Catalog |
| SQL Serving | Databricks SQL Warehouse |
| Visualization | Microsoft Power BI |
| Architecture | Medallion Architecture |
| Data Model | Star Schema |

---

# Project Structure

```text
.
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── raw/
│   │   └── .gitkeep
│   ├── bronze/
│   │   └── .gitkeep
│   ├── silver/
│   │   └── .gitkeep
│   ├── gold/
│   │   └── .gitkeep
│   └── rejected/
│       └── .gitkeep
│
├── notebooks/
│   ├── 01_raw_to_bronze.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_recompute_flight_duration.py
│   ├── 04_pii_masking.py
│   ├── 05_silver_to_gold_flights.py
│   └── 06_gold_kpi_summary.py
│
├── utils/
│   └── pii_masking.py
│
├── docs/
│   ├── azure_setup.md
│   └── ASG_Airlines_Documentation.docx
│
└── powerbi/
    ├── Neostat.pbix
    ├── dashboard_screenshot.png
    └── Powerbidashboard.pdf
```

> The `data/` directories are retained to document the pipeline layout, while operational source and generated data are excluded from Git to avoid exposing sensitive or environment-specific data.

---

# Dataset

The source workbook contains four sheets:

| Dataset | Raw Rows | Description |
|---|---:|---|
| `flights` | 1,020 | Flight identifiers, airline, route, departure/arrival information and duration |
| `bookings` | 1,000 | Booking information and passenger-related booking details |
| `passengers` | 1,039 | Passenger information including PII |
| `payments` | 1,000 | Payment amount and payment method |

The original source workbook is intentionally **not committed to the public repository** because it contains sensitive information.

---

# Pipeline

The pipeline is divided into six sequential stages:

```text
01 Raw → Bronze
       ↓
02 Bronze → Silver
       ↓
03 Recompute Flight Duration
       ↓
04 PII Masking
       ↓
05 Silver → Gold
       ↓
06 Gold KPI Summary
```

## 01 — Raw to Bronze

`notebooks/01_raw_to_bronze.py`

Responsibilities:

- Read the Excel source workbook
- Load all four source sheets
- Profile the raw datasets
- Inspect nulls and duplicates
- Write datasets as Delta tables
- Preserve the source structure without business transformations

## 02 — Bronze to Silver

`notebooks/02_bronze_to_silver.py`

Responsibilities:

- Remove exact duplicate flight records
- Detect conflicting flight identifiers
- Quarantine conflicting records
- Create missing-value indicator fields
- Remove the unreliable source duration field
- Standardize data for downstream processing
- Write cleaned Silver Delta tables

## 03 — Recompute Flight Duration

`notebooks/03_recompute_flight_duration.py`

Responsibilities:

- Parse departure and arrival timestamps
- Calculate duration using complete datetime values
- Correctly handle cross-day/overnight flights
- Detect negative-duration anomalies
- Preserve anomalies through an explicit flag

## 04 — PII Masking

`notebooks/04_pii_masking.py`

Responsibilities:

- Protect Aadhaar identifiers
- Protect passport numbers
- Mask email addresses
- Mask phone numbers
- Mask emergency-contact information
- Protect names where required
- Produce protected Silver datasets

Reusable masking and hashing logic is maintained in:

`utils/pii_masking.py`

## 05 — Silver to Gold

`notebooks/05_silver_to_gold_flights.py`

Responsibilities:

- Build the flight analytical fact table
- Build airline dimension
- Build route dimension
- Build date dimension
- Write Gold Delta tables

## 06 — Gold KPI Summary

`notebooks/06_gold_kpi_summary.py`

Responsibilities:

- Calculate global flight KPIs
- Generate a KPI summary table
- Provide baseline metrics for reporting validation

---

# Data Quality Issues Found and Handled

The pipeline profiles the source data and handles observed issues rather than assuming that every possible data problem exists.

| Issue | Dataset | Resolution |
|---|---|---|
| 15 exact duplicate flight rows | Flights | Removed, keeping one occurrence |
| Conflicting `flight_id` `6F250` | Flights | Conflicting records quarantined |
| Inconsistent raw duration formats | Flights | Source duration removed and recomputed |
| Missing airline/status/last name/amount | Multiple | Missingness retained through indicator fields |
| Aadhaar leading-zero issue | Passengers | Source columns ingested as strings |
| Negative duration for `SJ192` | Flights | Flagged as anomaly and retained |

---

# Flight Duration Calculation

The source `duration` field was not considered reliable enough to use as the authoritative measure.

Flight duration is recomputed using:

```text
Arrival Datetime - Departure Datetime
```

The calculation uses the complete date and time values.

This prevents incorrect results when a flight crosses midnight.

---

# Overnight Flight Handling

For example:

```text
Departure: 23:30 — Day 1
Arrival:   02:15 — Day 2
```

The complete datetime calculation gives:

```text
02:15 Day 2 - 23:30 Day 1
= 165 minutes
```

No manual 24-hour adjustment is required.

The final cleaned flight dataset contains:

- **122 overnight flights**
- **12.2% of clean flights**

---

# Anomaly Handling

The pipeline identified one negative-duration anomaly:

```text
flight_id = SJ192
```

The record is not silently corrected or deleted.

Instead, it is retained and explicitly flagged as an anomaly.

This follows a conservative data-quality principle:

> When the source does not provide enough information to determine the correct value, preserve the record and expose the quality issue rather than inventing a correction.

---

# PII Protection

PII protection is applied before sensitive passenger/booking information can reach the reporting layer.

### Hashing

The following identifiers are protected using salted SHA-256 hashing:

- `aadhaar_id`
- `passport_number`

The hashing salt is supplied through an environment variable rather than being hardcoded.

### Masking

The following fields are partially masked:

- `email`
- `phone`
- `emergency_contact_phone`

Emergency-contact names are also protected.

### Gold-Layer Privacy Boundary

The Gold layer contains **flight-level analytical data only**.

Passenger, booking, and payment-level information is not propagated into the Gold reporting model.

Therefore, the Power BI reporting layer does not expose passenger-level PII.

This provides privacy protection through both:

1. Data masking/hashing
2. Data-model minimization

---

# Gold Data Model

The Gold layer follows a flight-focused star schema.

```text
                   dim_airline
                       │
                       │
                       ▼
dim_date ───────► fact_flights ◄────── dim_route
```

### Fact Table

`fact_flights`

Contains flight-level analytical measures and attributes.

### Dimensions

`dim_airline`

Contains airline-related attributes.

`dim_route`

Contains source → destination route information.

`dim_date`

Provides date-based analytical attributes.

---

# Final Pipeline Results

The final validated flight dataset contains:

| Metric | Result |
|---|---:|
| Raw flight records | 1,020 |
| Clean flight records | 1,003 |
| Exact duplicates removed | 15 |
| Conflicting flight identifier | `6F250` |
| Average flight duration | 163.2 minutes |
| Overnight flights | 122 |
| Overnight flight percentage | 12.2% |
| Duration anomalies | 1 |
| Airlines | 5 |
| Routes | 30 |

These values are used as validation/reference metrics for the analytical output.

---

# Business KPIs

The Power BI solution focuses on the following business questions.

### Average Flight Duration

Measures the average duration of operated flights using the recomputed duration field.

### Route-wise Traffic

Measures flight volume for each:

`Source → Destination`

route.

### Airline Distribution

Shows the distribution and volume of flights across airlines.

### Delay / Anomaly Insights

The source dataset does not contain scheduled departure/arrival times required for conventional delay calculation.

Therefore, duration anomalies are used as the defensible operational anomaly proxy.

This limitation is explicitly documented rather than presenting duration anomalies as true scheduled delays.

---

# Power BI Dashboard

The Power BI report contains five pages:

1. **Overview**
2. **Duration Analysis**
3. **Route Performance**
4. **Airline Trends**
5. **Delay & Anomaly Insights**

The report connects to the Gold Delta tables exposed through Databricks Unity Catalog.

### Dashboard Deliverables

```text
powerbi/
├── Neostat.pbix
├── dashboard_screenshot.png
└── Powerbidashboard.pdf
```

The screenshot provides a quick preview of the report, while the PDF contains the exported dashboard pages.

---

# Running the Pipeline

## 1. Install Dependencies

Create a Python environment and install:

```bash
pip install -r requirements.txt
```

Main dependencies:

```text
pandas
numpy
deltalake
azure-storage-file-datalake
```

## 2. Configure Azure Access

Set the required Azure storage configuration, such as:

```text
ADLS_STORAGE_KEY
```

Alternatively, use the supported Azure authentication flow configured by the pipeline.

Refer to:

`docs/azure_setup.md`

for Azure connectivity setup.

## 3. Configure PII Salt

Before running the PII masking stage, configure:

```text
PII_SALT
```

Generate a cryptographically random salt using:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

Store the generated value securely and do not commit it to Git.

## 4. Run Pipeline Stages

Execute the six stages sequentially:

```text
01_raw_to_bronze.py
02_bronze_to_silver.py
03_recompute_flight_duration.py
04_pii_masking.py
05_silver_to_gold_flights.py
06_gold_kpi_summary.py
```

Each stage consumes the output of the previous stage.

---

# Security and Governance

The repository intentionally excludes:

- `.env` files
- Azure credentials
- Private keys
- Raw source datasets
- Generated local Delta tables
- Temporary development files

The `.gitignore` file provides repository-level protection against accidentally committing these files.

PII is additionally protected at the data-processing layer through hashing, masking, and Gold-layer data minimization.

---

# Scalability Considerations

The solution is designed around cloud storage and Delta tables rather than local-only storage.

### ADLS Gen2

Provides scalable centralized storage for the Raw, Bronze, Silver, and Gold layers.

### Delta Lake

Provides a structured table format suitable for analytical workloads and future distributed processing.

### Medallion Architecture

Separates:

```text
Raw preservation
      ↓
Bronze ingestion
      ↓
Silver quality and governance
      ↓
Gold analytics
```

This separation makes individual stages easier to test, maintain, rerun, and scale.

### Processing Migration

The current Python implementation can be migrated to a distributed processing engine such as Spark if larger data volumes require distributed computation.

The underlying Medallion Architecture and Gold data model can remain conceptually unchanged.

---

# Limitations

### Databricks Processing

Databricks cluster provisioning was not successfully completed because of repeated Azure capacity and worker-environment issues.

Therefore, the demonstrated ETL processing runs through native Python and Delta Lake tooling.

### Azure Data Factory

Azure Data Factory was part of the original planned architecture but was not implemented as the final orchestration layer.

The demonstrated pipeline is executed through six sequential processing stages.

### Delay Analysis

The source data does not contain scheduled flight times.

Therefore, the dashboard uses duration anomalies as an operational anomaly proxy rather than claiming to calculate true scheduled-flight delays.

### Data Availability

Raw and generated Delta datasets are excluded from the public GitHub repository because they may contain sensitive information and environment-specific data.

The repository retains the expected data-directory structure for reproducibility.

### Power BI Service

Publishing to Power BI Service is outside the scope of this submission.

The `.pbix`, dashboard screenshot, and PDF export are provided as the reporting deliverables.

---

# Future Enhancements

Potential production enhancements include:

- Azure Data Factory orchestration
- Distributed Spark processing through Databricks
- Automated pipeline scheduling
- Structured application logging
- Automated data-quality tests
- CI/CD validation
- Data lineage tracking
- Power BI Service deployment
- Incremental processing
- Automated monitoring and alerting

---

# Documentation

Detailed technical documentation is available at:

`docs/ASG_Airlines_Documentation.docx`

It covers:

- Dataset structure
- Architecture
- Data flow
- Data model
- Data-quality issues
- Cleaning logic
- Transformation logic
- Flight-duration calculation
- Overnight-flight handling
- PII protection
- KPI definitions
- Power BI dashboard walkthrough
- Privacy considerations
- Azure configuration
- Technology deviations
- Assumptions and limitations

Azure-specific setup information is available at:

`docs/azure_setup.md`

---

# Key Engineering Practices Demonstrated

This project demonstrates:

- End-to-end data ingestion
- Medallion Architecture
- Azure Data Lake Storage Gen2
- Delta Lake
- Data profiling
- Data-quality validation
- Duplicate handling
- Record quarantine
- Data standardization
- Calendar-aware datetime processing
- Overnight-flight handling
- Anomaly detection
- PII hashing
- PII masking
- Data minimization
- Star-schema modeling
- KPI generation
- Databricks Unity Catalog
- Power BI analytics
- Engineering under cloud-resource constraints

---

# Conclusion

The ASG Airlines pipeline transforms messy operational airline data into a governed, privacy-conscious, analytics-ready flight data platform.

The final solution combines:

```text
Azure Data Lake Storage Gen2
          +
Python / pandas
          +
Delta Lake
          +
Databricks Unity Catalog
          +
Power BI
```

to provide a complete workflow from source ingestion through data-quality processing, PII protection, analytical modeling, KPI generation, and business reporting.

The implementation prioritizes **correctness, traceability, data quality, privacy, scalability, and transparent engineering decisions** rather than overstating infrastructure that could not be successfully provisioned in the available Azure environment.
