# Databricks notebook source
# MAGIC %md
# MAGIC Notebooks demonstrates the usage of dq-suite-amsterdam.
# MAGIC
# MAGIC **Requirements** to make this run:
# MAGIC - Create and start a personal compute
# MAGIC - Create the output tables using this script: https://github.com/Amsterdam/dq-suite-amsterdam/blob/main/scripts/data_quality_tables.sql
# MAGIC - Uploaded data quality rules are defined in here `dq_workshop.json`
# MAGIC
# MAGIC  

# COMMAND ----------

# Install dq-suite
%pip install dq-suite-amsterdam==0.11.10
 

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- explore the data 
# MAGIC SELECT * FROM samples.nyctaxi.trips LIMIT 10
# MAGIC  

# COMMAND ----------

# MAGIC  
# MAGIC %md
# MAGIC ## Run dq-suite validation
# MAGIC  

# COMMAND ----------

# path to json script
# -----replace with your own path
dq_rule_json_path = "/Workspace/Users/a.user@amsterdam.nl/dq_workshop.json"
 

# COMMAND ----------

# load data into a pyspark dataframe
df = spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 1000")
df.summary().show()
 

# COMMAND ----------

from dq_suite.validation import run_validation
 
# run the dataframe validation given the rules defined in dq_rule_json_path
run_validation(
    json_path=dq_rule_json_path,
    df=df,
    spark_session=spark,
    catalog_name="dpd1_dev", # -----replace with your own catalog name
    table_name="nyc_taxi",
    validation_name="dq_workshop",
)
 

# COMMAND ----------

# MAGIC  
# MAGIC %md
# MAGIC ## Explore output
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check regel van json defined in dq_rule_json_path
# MAGIC SELECT * FROM dpxx_dev.data_quality.regel
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check validatie output van run_validation()
# MAGIC SELECT * FROM dpxx_dev.data_quality.validatie
# MAGIC  

# COMMAND ----------

# MAGIC  
# MAGIC %md
# MAGIC ## Exercise
# MAGIC - Check which rules could be adjusted in dq_workshop.json file to make it work with the data
# MAGIC - Add an additional rule to be checked
# MAGIC - Check how the results are updated after multiple runs
