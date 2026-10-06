# Healthcare GenAI Data Platform — End-to-End v2

A production-style learning project using **ADF + ADLS Gen2 + Databricks + PySpark + Delta Lake + optional GenAI**.

> Synthetic healthcare data only. Do not use real patient/PHI data.s

## Final architecture

```text
SOURCE
  |
  | ADF Copy
  v
RAW  (immutable source snapshot / traceability)
  |
  | content-aware control check
  v
INGESTED  (active processing queue)
  |
  v
DATABRICKS
  |
  +--> BRONZE (append-only Delta + lineage)
  |
  +--> SILVER (cleaned / validated)
  |
  +--> GOLD (business-ready)
  |
  v
ARCHIVE  (only after successful processing)
```

### Duplicate/idempotency design

The project uses a Delta `control/file_control` table with:

- file name
- SHA-256 content hash
- file size
- source/raw/ingested/archive paths
- run id/date
- processing status
- timestamps
- error message

Rules:

```text
NEW / FAILED content -> INGESTED -> process
SUCCESS content      -> SKIP
processing failure   -> stay available for retry
successful run       -> ARCHIVE
```

The same content can be received again and retained in RAW for traceability, but the same hash with `SUCCESS` is not copied into INGESTED.

## ADLS layout

Container:

```text
healthcare
```

Folders:

```text
source/
raw/
ingested/
archive/
bronze/
silver/
gold/
control/
```

The source folder is the simulated upstream source for this repository.

## Active tables

The first end-to-end pipeline processes these six tables:

```text
patients
encounters
conditions
providers
payers
procedures
```

The repository also contains additional Synthea CSVs under `sources/`. They are preserved as source data but are not part of the first transformation model. Add them to `project_config.json` and extend the Silver/Gold notebooks when you are ready.

## Azure setup

### 1. Create ADLS Gen2

Create an Azure Storage Account with:

- Performance: Standard
- Redundancy: LRS for learning
- Hierarchical namespace: Enabled

Create container:

```text
healthcare
```

Create folders:

```text
source
raw
ingested
archive
bronze
silver
gold
control
```

### 2. Put the six source files in ADLS

Copy:

```text
sources/patients.csv
sources/encounters.csv
sources/conditions.csv
sources/providers.csv
sources/payers.csv
sources/procedures.csv
```

to:

```text
abfss://healthcare@<storageaccount>.dfs.core.windows.net/source/
```

Do not put them directly into RAW. ADF is responsible for the Source -> Raw copy.

### 3. Deploy the Databricks code

Create/import a Databricks Repo and place the project under:

```text
/Repos/healthcare_Gen_project-
```

The notebooks are:

```text
00_config
01_control_and_ingest
02_bronze_ingest
03_silver_transform
04_gold_model
05_archive_processed
06_genai_insights (optional)
```

Create a small Azure Databricks compute resource and use an LTS runtime.

### 4. ADLS access

Give the Databricks access identity the required Storage Blob Data Contributor permission on the storage account/container.

For ADF, use a managed identity where possible. Grant the ADF identity the required storage role.

The JSON linked services in `adf/linkedService/` are templates. Replace placeholders with your actual workspace/storage/Key Vault details or create the linked services in ADF Studio.

### 5. Databricks linked service

`adf/linkedService/ls_databricks.json` uses a Key Vault-backed Databricks token template.

Create:

```text
Key Vault
  secret: databricks-token
```

and configure:

```text
ls_keyvault
```

Alternatively, configure the ADF Databricks linked service using your organization's preferred managed-identity/service-principal method.

### 6. Import ADF assets

Import/deploy:

```text
adf/linkedService/
adf/dataset/
adf/pipeline/pl_healthcare_end_to_end.json
```

The pipeline parameters include:

```text
tables
lake_root
```

Set:

```text
lake_root =
abfss://healthcare@<storageaccount>.dfs.core.windows.net
```

### 7. Pipeline execution

The pipeline performs:

1. Set run date and unique run id.
2. Copy `source/*.csv` -> `raw/table=.../received_date=.../run_id=.../`.
3. Call `01_control_and_ingest`.
4. Compute content hashes and check the Delta control table.
5. Copy only new/failed files -> `ingested/`.
6. Run Bronze.
7. Run Silver.
8. Run Gold.
9. Run `05_archive_processed`.
10. Copy -> Archive, verify, then delete the active Ingested copy.
11. Mark the control record `SUCCESS`.

If any processing notebook fails before archive, the ingested file remains available for retry.

## Databricks layer responsibilities

### Bronze

- Reads only files registered by the control gate.
- Keeps incoming columns as strings.
- Adds `_source_file`, `_source_path`, `_file_hash`, `_ingest_ts`, `_run_id`.
- Writes Delta.

### Silver

- Cleans and standardizes data.
- Removes duplicates.
- Converts dates/timestamps/numeric columns.
- Applies basic data-quality flags.
- Drops direct patient identifiers such as SSN/driver/passport from the modeled patient table.
- Writes Delta.

### Gold

Creates:

```text
dim_date
dim_patient
dim_provider
dim_payer
fact_encounters
agg_readmission_30d
agg_cost_by_condition
agg_utilization_monthly
```

These are the business-ready datasets for analytics/GenAI.

## Optional GenAI

`databricks/06_genai_insights.py` generates an executive summary from aggregated Gold KPIs only.

It does not send patient rows to the LLM.

For the Streamlit SQL assistant, see:

```text
genai/
```

The GenAI app uses Synapse serverless SQL in the original design. Treat that as an optional downstream layer after Gold is stable.

## Recommended implementation order

```text
1. ADLS
2. Upload source files
3. Databricks access
4. Run 00_config
5. Test 01_control_and_ingest
6. Run Bronze
7. Run Silver
8. Run Gold
9. Validate Gold
10. Configure ADF
11. Run complete ADF pipeline
12. Test duplicate file
13. Test failure/retry
14. Test archive
15. Add monitoring
16. Add GenAI
```

## Duplicate test

Run the pipeline once.

Then place the same source file in `source/` again.

Expected:

```text
RAW: new source arrival is retained
CONTROL: same SHA-256 already has SUCCESS
INGESTED: duplicate is not created
DATABRICKS: no duplicate processing
ARCHIVE: no duplicate processing
```

Then change one byte/record in the file and send it again.

Expected:

```text
new SHA-256
      |
      v
INGESTED
      |
      v
processed as a new version/batch
```

## Cost control

For learning:

- Use a small Databricks cluster.
- Enable auto-termination.
- Do not leave compute running.
- Avoid unnecessary ADF triggers.
- Use LRS for non-production learning.
- Delete unused resources after the project.

## Important

This repository is a learning/reference implementation. Before production use, add:

- Unity Catalog governance
- Managed identities/service principals
- Key Vault
- formal schema contracts
- stronger data-quality rules
- alerting/monitoring
- CI/CD
- retry/dead-letter design
- proper PHI/security controls if real healthcare data is ever introduced
