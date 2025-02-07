# Databricks notebook source
pip install dq-suite-amsterdam==0.11.10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.nyctaxi.trips LIMIT 10

# COMMAND ----------

df = spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 1000")

# COMMAND ----------

dq_rule_json_path = "/Workspace/Users/a.user@amsterdam.nl/dq_workshop.json"

# COMMAND ----------

from dq_suite.validation import run_validation

run_validation(
    json_path=dq_rule_json_path,
    df=df,
    spark_session=spark,
    catalog_name="dpxx_dev",
    table_name="nyc_taxi",
    validation_name="dq_workshop",
)
