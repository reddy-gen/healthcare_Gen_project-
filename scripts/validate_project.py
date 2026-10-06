from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]

required = [
    "project_config.json",
    "README.md",
    "adf/pipeline/pl_healthcare_end_to_end.json",
    "adf/dataset/ds_source_binary.json",
    "adf/dataset/ds_raw_binary.json",
    "adf/linkedService/ls_adls.json",
    "adf/linkedService/ls_databricks.json",
    "databricks/00_config.py",
    "databricks/01_control_and_ingest.py",
    "databricks/02_bronze_ingest.py",
    "databricks/03_silver_transform.py",
    "databricks/04_gold_model.py",
    "databricks/05_archive_processed.py"
]

missing = [x for x in required if not (ROOT / x).exists()]
if missing:
    raise SystemExit("Missing: " + ", ".join(missing))

for p in [
    ROOT / "project_config.json",
    ROOT / "adf/pipeline/pl_healthcare_end_to_end.json"
]:
    json.loads(p.read_text(encoding="utf-8"))

print("Project structure validation: PASS")
