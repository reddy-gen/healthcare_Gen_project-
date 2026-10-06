# Databricks notebook source
# Ingest only files registered by the control gate for this run.
# Bronze preserves the incoming columns as strings and adds lineage metadata.

# MAGIC %run ./00_config

from delta.tables import DeltaTable

control_df = spark.read.format("delta").load(f"{CONTROL}/file_control") \
    .filter((F.col("run_id") == RUN_ID) & (F.col("status") == "INGESTED"))

for row in control_df.select("table_name", "ingested_path", "file_hash", "file_name").collect():
    table = row["table_name"]
    path = row["ingested_path"]

    df = (spark.read.option("header", True).option("inferSchema", False).csv(path)
          .withColumn("_source_file", F.lit(row["file_name"]))
          .withColumn("_source_path", F.lit(path))
          .withColumn("_file_hash", F.lit(row["file_hash"]))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_run_id", F.lit(RUN_ID))
          .withColumn("_run_date", F.lit(RUN_DATE)))

    target = f"{BRONZE}/{table}"
    df.write.format("delta").mode("append").save(target)

    (DeltaTable.forPath(spark, f"{CONTROL}/file_control")
      .update(
          condition=(F.col("file_hash") == row["file_hash"]) & (F.col("run_id") == RUN_ID),
          set={"status": F.lit("BRONZE_SUCCESS"),
               "processing_start_ts": F.current_timestamp()}
      ))
    print(f"Bronze complete: {table} <- {path}")

print("Bronze layer complete.")
