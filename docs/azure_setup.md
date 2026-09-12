# Azure Services Connectivity & Architecture Setup Guide

This document outlines the authentication, security, and networking configuration required to connect the components in the ASG Airlines data pipeline.

---

## 1. ADLS Gen2 ↔ Azure Databricks

### Purpose
Allows Databricks (and, in this project's actual implementation, local Python processes) to read raw data and read/write Delta tables across the `raw`, `bronze`, `silver`, and `gold` storage containers.

### Integration Mechanism: Storage Account Access Key via Databricks Secrets
To maintain security and prevent hardcoding credentials in notebooks, the Storage Account access key is stored securely in an Azure Databricks Secret Scope.

### Configuration Steps:
1. **Retrieve Storage Access Key**:
   - Navigate to the Storage Account in the Azure Portal or Azure CLI.
   - Go to **Security + networking** > **Access keys**.
   - Copy `Key 1` (or `Key 2`).

2. **Create Databricks Secret Scope**:
   - Using the Databricks CLI:
     ```bash
     databricks secrets create-scope --scope adls-scope
     databricks secrets put --scope adls-scope --key storage-account-key
     ```
   - Enter the copied storage access key when prompted.

3. **Access ADLS Gen2 in Notebooks (PySpark)**:
   ```python
   storage_account_name = "stasgairlines01"
   storage_key = dbutils.secrets.get(scope="adls-scope", key="storage-account-key")

   spark.conf.set(
       f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
       storage_key
   )

   raw_path = f"abfss://raw@{storage_account_name}.dfs.core.windows.net/"
   bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"
   silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"
   gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
   ```

4. **Actual local-execution equivalent (what this project runs today)**:
   Since processing was moved to local Python (see Section 4 below), the same
   storage account key is instead read from an environment variable and passed
   directly to the `deltalake` library's `storage_options`, using the `az://`
   URI scheme:
   ```python
   storage_options = {
       "azure_storage_account_name": "stasgairlines01",
       "azure_storage_access_key": os.environ["ADLS_STORAGE_KEY"],
   }
   write_deltalake("az://bronze/flights", df, storage_options=storage_options)
   ```
   The key is never hardcoded in any script — it is read from the
   `ADLS_STORAGE_KEY` environment variable, with a fallback to fetching it live
   via `az storage account keys list` if the variable isn't set.

---

## 2. Azure Data Factory (ADF) ↔ Azure Databricks

### Purpose
Enables Azure Data Factory to trigger and orchestrate PySpark notebook runs sequentially (Ingestion → Bronze → Silver → Gold) and monitor pipeline execution.

### Integration Mechanism: Databricks Linked Service via Personal Access Token (PAT)
ADF connects to Azure Databricks using a dedicated Linked Service authenticated via a Personal Access Token (or Azure Managed Identity).

### Configuration Steps:
1. **Generate Databricks Personal Access Token (PAT)**:
   - In the Databricks Workspace, go to **User Settings** > **Developer** > **Access tokens**.
   - Click **Generate new token**, specify a lifetime and comment (e.g. `ADF-Orchestrator-Token`), and copy the token.

2. **Create Linked Service in Azure Data Factory**:
   - In ADF Studio, navigate to **Manage** > **Linked Services** > **New**.
   - Select **Azure Databricks**.
   - Set:
     - **Databricks workspace**: Select the provisioned workspace.
     - **Authentication type**: Access Token (or Azure Key Vault secret reference).
     - **Access token**: Paste the Databricks PAT.
     - **Select cluster**: Existing Interactive Cluster or Job Cluster (Standard VM, 0 workers / single node for cost efficiency).
   - Click **Test Connection** and **Save/Publish**.

3. **Orchestrate Pipelines**:
   - In ADF authoring, drag **Databricks Notebook** activities onto the canvas.
   - Link each activity to the Databricks Linked Service and reference the corresponding workspace notebook paths:
     - Notebook 1: `01_raw_to_bronze`
     - Notebook 2: `02_bronze_to_silver`
     - Notebook 3: `03_silver_to_gold`

> **Note:** ADF orchestration as described above was designed but never actually
> implemented in this project — see Section 4 for why, and what runs in its place.

---

## 3. Databricks SQL Warehouse ↔ Power BI

### Purpose
Serves Gold-layer Delta tables and business aggregations directly to Power BI desktop reports and service dashboards with high performance and low query latency.

### Integration Mechanism: Native Azure Databricks Connector
Power BI Desktop and Power BI Service include an optimized native connector for Databricks SQL Warehouses (utilizing DirectQuery or Import mode).

### Configuration Steps:
1. **Obtain Connection Details from Databricks SQL**:
   - In Databricks, navigate to **SQL Warehouses** (or compute endpoint).
   - Go to the **Connection details** tab:
     - **Server Hostname**: `adb-<workspace-id>.<number>.azuredatabricks.net`
     - **HTTP Path**: `sql/protocolv1/o/<workspace-id>/<warehouse-id>`
     - **Port**: `443`

2. **Connect from Power BI**:
   - Open Power BI Desktop and select **Get Data** > **Azure** > **Azure Databricks**.
   - Enter the **Server Hostname** and **HTTP Path**.
   - Select **DirectQuery** (for live querying) or **Import** mode.
   - For Authentication, select **Azure Active Directory** (Organizational Account) or **Personal Access Token**.

3. **Select Gold Tables**:
   - Browse the catalog/schema to select the gold dimensional and fact tables (e.g., `fact_flights`, `dim_airline`, `dim_route`, `dim_date`, `gold_kpi_summary`).
   - Build data models, DAX measures, and visual dashboards.

This connector is used exactly as designed in this project — the Gold Delta
tables produced by the local pipeline are registered as external Unity Catalog
tables (via a Managed Identity storage credential and an external location,
`gold_location`, pointing at the `gold` container), and Power BI connects to
them through a Databricks SQL Warehouse as described above.

---

## 4. Technology Deviation: Why Processing Runs Locally, Not on Databricks

The original architecture (per the case study's preferred recommendation) used
Azure Databricks (PySpark) for all ingestion/cleaning/transformation, with
Azure Data Factory orchestrating notebook execution. During implementation,
Databricks **cluster (compute) provisioning failed repeatedly** across three
Azure regions on this Azure for Students subscription:

- **`centralindia`**: cluster stuck in `PENDING` ("Finding instances for new
  nodes, acquiring more instances if necessary") across multiple VM sizes
  (`Standard_DS3_v2`, `Standard_D4s_v3`, `Standard_D4s_v4`) — confirmed as an
  Azure regional capacity/stockout issue, not a quota problem (quota was
  available: 6 regional vCPUs, 4 per family).
- **`uaenorth`**: cluster reached `TERMINATED` with error
  `INVALID_WORKER_ENVIRONMENT: WORKER_ENVIRONMENT_NOT_FOUND`.
- **`koreacentral`**: cluster creation failed with `"Current organization ...
  does not have any associated worker environments"`.
- A manual workaround (opening the workspace once via the Azure Portal
  "Launch Workspace" action, to trigger backend activation) was tried on the
  `centralindia` workspace; the same `PENDING`/stockout behavior persisted
  afterward.

**Decision:** Databricks compute was abandoned for the ingestion/cleaning/
transformation pipeline. ADLS Gen2 was kept as the storage layer exactly as
planned. All pipeline stages instead run as local Python scripts/notebooks
(pandas + the `deltalake`/delta-rs library), reading and writing Delta tables
directly to ADLS Gen2 via the Azure SDK, as shown in Section 1 above. This is
consistent with the case study's own stated allowance for a local Python/SQL
alternative to Azure Databricks.

Azure Data Factory orchestration (Section 2) was never actually implemented,
for a related reason: ADF was intended to orchestrate Databricks notebook
execution, and the pipeline never got past establishing working Databricks
compute in the first place. All six pipeline stages are instead run as
discrete, independently-validated notebooks, each checked (row counts, sample
output, validation queries) before proceeding to the next.

Databricks is still used, but only downstream, as a query/serving layer
(Section 3) — Power BI connects to the Gold Delta tables via Databricks SQL
Warehouse and Unity Catalog, exactly as originally designed.

**Cost note:** because Databricks compute was never successfully provisioned
for processing, no meaningful compute cost was incurred there. The only
Databricks cost exposure in this project is the SQL Warehouse used to serve
the Gold tables to Power BI.
