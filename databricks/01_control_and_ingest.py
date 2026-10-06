# Databricks notebook source
# Content-aware idempotency gate.
#
# ADF has already copied SOURCE -> RAW for this run.
# This notebook:
# 1) scans RAW,
# 2) computes SHA-256 for each source file,
# 3) checks the Delta control table,
# 4) copies only NEW/FAILED files to INGESTED,
# 5) records the file state.
#
# A duplicate file can remain in RAW for traceability, but it will not be
# copied into INGESTED when the same content hash already has SUCCESS status.

# MAGIC %run ./00_config

from datetime import datetime
from delta.tables import DeltaTable

CONTROL_PATH = f"{CONTROL}/file_control"

control_schema = """
file_id string,
file_name string,
table_name string,
file_hash string,
file_size long,
raw_path string,
ingested_path string,
archive_path string,
run_id string,
run_date string,
status string,
received_ts timestamp,
processing_start_ts timestamp,
processing_end_ts timestamp,
error_message string
"""

if not DeltaTable.isDeltaTable(spark, CONTROL_PATH):
    spark.createDataFrame([], control_schema).write.format("delta").save(CONTROL_PATH)

control = spark.read.format("delta").load(CONTROL_PATH)
success_hashes = {
    r["file_hash"] for r in control.filter("status = 'SUCCESS'")
    .select("file_hash").distinct().collect()
}

def sha256_file(path):
    # dbutils.fs.open is not available on every runtime for binary reads.
    # dbutils.fs.head is intentionally avoided for large files.
    # Use Spark binaryFile to calculate the content hash reliably.
    b = (spark.read.format("binaryFile")
         .load(path)
         .select("path", "length", F.sha2(F.col("content"), 256).alias("sha256"))
         .limit(1)
         .collect())
    if not b:
        raise FileNotFoundError(path)
    return b[0]["sha256"], b[0]["length"]

# Find raw CSVs for the active tables. Each ADF run is stored under a unique run_id.
raw_files = []
for table in TABLES:
    pattern = f"{RAW}/{table}/"
    try:
        rows = (spark.read.format("binaryFile")
                .option("recursiveFileLookup", "true")
                .load(pattern)
                .filter("path LIKE '%.csv'")
                .select("path", "length", "modificationTime"))
        for r in rows.collect():
            raw_files.append((table, r["path"], r["length"], r["modificationTime"]))
    except Exception as exc:
        print(f"No RAW files for {table}: {exc}")

new_count = 0
skipped_count = 0

for table, raw_path, file_size, modified_ts in raw_files:
    file_name = raw_path.rsplit("/", 1)[-1]
    file_hash = (spark.read.format("binaryFile").load(raw_path)
                 .select(F.sha2("content", 256).alias("hash"))
                 .first()["hash"])

    already_success = file_hash in success_hashes

    # Only the current pipeline run is eligible for ingestion. This prevents
    # historical RAW files from being re-read on every run.
    current_run = f"/run_id={RUN_ID}/" in raw_path

    if not current_run:
        continue

    ingested_path = f"{INGESTED}/run_id={RUN_ID}/{table}/{file_name}"
    archive_path = f"{ARCHIVE}/table={table}/run_id={RUN_ID}/{file_name}"

    if already_success:
        skipped_count += 1
        print(f"SKIP duplicate: {file_name} hash={file_hash}")
        continue

    # New or previously failed content: make it available for processing.
    dbutils.fs.mkdirs(ingested_path.rsplit("/", 1)[0])
    dbutils.fs.cp(raw_path, ingested_path, True)

    row = [(f"{table}:{file_hash}", file_name, table, file_hash, int(file_size),
            raw_path, ingested_path, archive_path, RUN_ID, RUN_DATE, "INGESTED",
            datetime.utcnow(), None, None, None)]
    (spark.createDataFrame(row, control_schema)
          .write.format("delta").mode("append").save(CONTROL_PATH))
    success_hashes.add(file_hash)
    new_count += 1

print(f"New/eligible files copied to INGESTED: {new_count}")
print(f"Duplicates skipped: {skipped_count}")

dbutils.notebook.exit(json.dumps({
    "run_id": RUN_ID,
    "new_files": new_count,
    "duplicates_skipped": skipped_count
}))
