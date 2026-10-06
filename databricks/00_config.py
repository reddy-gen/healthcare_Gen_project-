# Databricks notebook source
# Common configuration for the end-to-end healthcare pipeline.
# ADF passes lake_root, run_id and run_date to the notebooks.

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

dbutils.widgets.text("lake_root", "abfss://healthcare@<storageaccount>.dfs.core.windows.net")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_date", "")

LAKE = dbutils.widgets.get("lake_root").rstrip("/")
RUN_ID = dbutils.widgets.get("run_id")
RUN_DATE = dbutils.widgets.get("run_date")

RAW = f"{LAKE}/raw"
INGESTED = f"{LAKE}/ingested"
ARCHIVE = f"{LAKE}/archive"
BRONZE = f"{LAKE}/bronze"
SILVER = f"{LAKE}/silver"
GOLD = f"{LAKE}/gold"
CONTROL = f"{LAKE}/control"

TABLES = ["patients", "encounters", "conditions", "providers", "payers", "procedures"]

def table_path(zone, table):
    return f"{zone}/{table}"

def dedupe(df, key, order_col="_ingest_ts"):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

print(f"LAKE={LAKE}")
print(f"RUN_ID={RUN_ID}")
print(f"RUN_DATE={RUN_DATE}")
