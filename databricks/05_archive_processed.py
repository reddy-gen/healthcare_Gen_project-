# Databricks notebook source
# Archive only files that completed Bronze/Silver/Gold successfully.
# Safer pattern: COPY -> VERIFY -> DELETE from INGESTED.

# MAGIC %run ./00_config

from delta.tables import DeltaTable

CONTROL_PATH = f"{CONTROL}/file_control"
control = spark.read.format("delta").load(CONTROL_PATH)
run_rows = control.filter((F.col("run_id") == RUN_ID) & (F.col("status") == "INGESTED")) \
                  .select("file_hash", "ingested_path", "archive_path", "file_name", "table_name").collect()

for r in run_rows:
    # If Gold reached this notebook, all transformations for the run succeeded.
    dbutils.fs.mkdirs(r["archive_path"].rsplit("/", 1)[0])
    dbutils.fs.cp(r["ingested_path"], r["archive_path"], True)

    # Verify archive exists before deleting active input.
    archived = [x.path for x in dbutils.fs.ls(r["archive_path"].rsplit("/", 1)[0])
                if x.path.rstrip("/") == r["archive_path"].rstrip("/")]
    if not archived:
        raise RuntimeError(f"Archive verification failed: {r['archive_path']}")

    dbutils.fs.rm(r["ingested_path"], True)

    (DeltaTable.forPath(spark, CONTROL_PATH)
      .update(
          condition=(F.col("file_hash") == r["file_hash"]) & (F.col("run_id") == RUN_ID),
          set={"status": F.lit("SUCCESS"),
               "processing_end_ts": F.current_timestamp(),
               "error_message": F.lit(None).cast("string")}
      ))
    print(f"Archived successfully: {r['file_name']}")

print("Archive step complete.")
