# ADF assets

Use:

```text
linkedService/ls_adls.json
linkedService/ls_keyvault.json
linkedService/ls_databricks.json
dataset/ds_source_binary.json
dataset/ds_raw_binary.json
pipeline/pl_healthcare_end_to_end.json
```

The older `landing` pipeline from v1 is superseded by the v2 pipeline.

The v2 pipeline deliberately uses:

```text
SOURCE -> RAW -> CONTROL/IDEMPOTENCY -> INGESTED -> DATABRICKS -> ARCHIVE
```

ADF orchestrates; Databricks performs content-aware duplicate control and transformations.
