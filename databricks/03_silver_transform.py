# Databricks notebook source
# Clean, standardize and validate the healthcare data.

# MAGIC %run ./00_config

def read_bronze(table):
    return spark.read.format("delta").load(f"{BRONZE}/{table}")

# PATIENTS
p = read_bronze("patients")
p = dedupe(p, "Id")
p = (p.select(
        F.col("Id").alias("patient_id"),
        F.to_date("BIRTHDATE").alias("birth_date"),
        F.to_date("DEATHDATE").alias("death_date"),
        F.sha2(F.concat_ws(" ", "FIRST", "LAST"), 256).alias("name_hash"),
        F.upper("GENDER").alias("gender"),
        F.col("RACE").alias("race"),
        F.col("ETHNICITY").alias("ethnicity"),
        F.col("MARITAL").alias("marital_status"),
        F.initcap("CITY").alias("city"),
        F.upper("STATE").alias("state"),
        F.col("COUNTY").alias("county"),
        F.col("ZIP").alias("zip"),
        F.col("INCOME").cast("double").alias("income"),
        F.col("HEALTHCARE_EXPENSES").cast("double").alias("healthcare_expenses"),
        F.col("HEALTHCARE_COVERAGE").cast("double").alias("healthcare_coverage"),
        "_source_file", "_file_hash", "_ingest_ts")
     .filter("patient_id IS NOT NULL"))
p.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/patients")

# ENCOUNTERS
e = dedupe(read_bronze("encounters"), "Id")
e = (e.select(
        F.col("Id").alias("encounter_id"),
        F.col("PATIENT").alias("patient_id"),
        F.col("PROVIDER").alias("provider_id"),
        F.col("PAYER").alias("payer_id"),
        F.col("ORGANIZATION").alias("organization_id"),
        F.to_timestamp("START").alias("start_ts"),
        F.to_timestamp("STOP").alias("stop_ts"),
        F.lower("ENCOUNTERCLASS").alias("encounter_class"),
        F.col("CODE").alias("encounter_code"),
        F.col("DESCRIPTION").alias("encounter_desc"),
        F.col("BASE_ENCOUNTER_COST").cast("double").alias("base_cost"),
        F.col("TOTAL_CLAIM_COST").cast("double").alias("total_claim_cost"),
        F.col("PAYER_COVERAGE").cast("double").alias("payer_coverage"),
        F.col("REASONCODE").alias("reason_code"),
        F.col("REASONDESCRIPTION").alias("reason_desc"),
        "_source_file", "_file_hash", "_ingest_ts")
     .filter("encounter_id IS NOT NULL AND patient_id IS NOT NULL AND start_ts IS NOT NULL")
     .withColumn("dq_negative_cost", F.coalesce(F.col("total_claim_cost"), F.lit(0)) < 0)
     .withColumn("dq_stop_before_start", F.col("stop_ts") < F.col("start_ts")))
e.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/encounters")

# CONDITIONS
c = (read_bronze("conditions")
     .select(F.col("PATIENT").alias("patient_id"),
             F.col("ENCOUNTER").alias("encounter_id"),
             F.col("CODE").alias("snomed_code"),
             F.col("DESCRIPTION").alias("condition_desc"),
             F.to_date("START").alias("start_date"),
             F.to_date("STOP").alias("stop_date"),
             "_source_file", "_file_hash")
     .dropDuplicates(["patient_id", "encounter_id", "snomed_code"]))
c.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/conditions")

# PROVIDERS
pr = dedupe(read_bronze("providers"), "Id").select(
    F.col("Id").alias("provider_id"),
    F.col("ORGANIZATION").alias("organization_id"),
    F.col("NAME").alias("provider_name"),
    F.col("SPECIALITY").alias("specialty"),
    F.col("CITY").alias("city"),
    F.col("STATE").alias("state"))
pr.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/providers")

# PAYERS
py = dedupe(read_bronze("payers"), "Id").select(
    F.col("Id").alias("payer_id"),
    F.col("NAME").alias("payer_name"))
py.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/payers")

# PROCEDURES
pc = (read_bronze("procedures")
      .select(F.col("PATIENT").alias("patient_id"),
              F.col("ENCOUNTER").alias("encounter_id"),
              F.col("CODE").alias("procedure_code"),
              F.col("DESCRIPTION").alias("procedure_desc"),
              F.col("BASE_COST").cast("double").alias("base_cost"),
              "_source_file", "_file_hash"))
pc.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/procedures")

dq = spark.read.format("delta").load(f"{SILVER}/encounters").agg(
    F.count("*").alias("rows"),
    F.sum(F.col("dq_negative_cost").cast("int")).alias("negative_cost_rows"),
    F.sum(F.col("dq_stop_before_start").cast("int")).alias("stop_before_start_rows"))
dq.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{SILVER}/_dq_summary")

print("Silver layer complete.")
display(dq)
