# Introduction 
This repository contains functions that will ease the use of Great Expectations. Users can input data and data quality rules and get results in return.

DISCLAIMER: The package is in MVP phase


# Getting started
Install the dq suite on your compute, for example by running the following code in your workspace:

```
pip install dq-suite-amsterdam
```

To validate your first table:
- define `dq_rule_json_path` as a path to a JSON file, similar to shown in dq_rules_example.json in this repo
- define `table_name` as the name of the table for which a data quality check is required. This name should also occur in the JSON file
- load the table requiring a data quality check into a PySpark dataframe `df` (e.g. via `spark.read.csv` or `spark.read.table`)

```python
import dq_suite

validation_settings_obj = dq_suite.ValidationSettings(spark_session=spark, 
                                                      catalog_name="dpxx_dev",
                                                      table_name=table_name,
                                                      check_name="name_of_check_goes_here")
dq_suite.run(json_path=dq_rule_json_path, df=df, validation_settings_obj=validation_settings_obj)
```
Looping over multiple data frames may require a redefinition of the `json_path` and `validation_settings` variables. 

See the documentation of `ValidationSettings` for what other parameters can be passed upon intialisation (e.g. Slack 
or MS Teams webhooks for notifications, location for storing GX, etc). 


# Create data quality schema and tables (in respective catalog of data team)
Before running your first dq check, create the data quality schema and tables from the notebook from repo path: scripts/data_quality_tables.sql
- Open the notebook, connect to a cluster.
- Select the catalog of the data team and execute the notebook. It will create the schema and tables if they are not yet there.


# Export the schema from Unity Catalog to the Input Form
In order to output the schema from Unity Catalog, use the following commands (using the required schema name):

```
schema_output = dq_suite.schema_to_json_string('schema_name', spark)
print(schema_output)
```

Copy the string to the Input Form to quickly ingest the schema in Excel.


# Validate the schema of a table
It is possible to validate the schema of an entire table to a schema definition from Amsterdam Schema in one go. This is done by adding two fields to the "dq_rules" JSON when describing the table (See: https://github.com/Amsterdam/dq-suite-amsterdam/blob/main/dq_rules_example.json). 

You will need:
- validate_table_schema: the id field of the table from Amsterdam Schema
- validate_table_schema_url: the url of the table or dataset from Amsterdam Schema

The schema definition is converted into column level expectations (expect_column_values_to_be_of_type) on run time.


# Known exceptions
- The functions can run on Databricks using a Personal Compute Cluster or using a Job Cluster. 
Using a Shared Compute Cluster will result in an error, as it does not have the permissions that Great Expectations requires.

- Since this project requires Python >= 3.10, the use of Databricks Runtime (DBR) >= 13.3 is needed 
([click](https://docs.databricks.com/en/release-notes/runtime/13.3lts.html#system-environment)). 
Older versions of DBR will result in errors upon install of the `dq-suite-amsterdam` library.

- At time of writing (late Aug 2024), Great Expectations v1.0.0 has just been released, and is not (yet) compatible with Python 3.12. Hence, make sure you are using the correct version of Python as interpreter for your project. 

# Contributing to this library
See the separate [developers' readme](src/Readme-dev.md).


# Updates
Version 0.1: Run a DQ check for a dataframe

Version 0.2: Run a DQ check for multiple dataframes

Version 0.3: Refactored I/O

Version 0.4: Added schema validation with Amsterdam Schema per table

Version 0.5: Export schema from Unity Catalog

Version 0.6: The results are written to tables in the "dataquality" schema

Version 0.7: Refactored the solution

Version 0.8: Implemented output historization

Version 0.9: Added dataset descriptions

Version 0.10: Switched to GX 1.0
