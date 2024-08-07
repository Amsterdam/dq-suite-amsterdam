# Introduction 
This repository contains functions that will ease the use of Great Expectations. Users can input data and data quality rules and get results in return.

DISCLAIMER: The package is in MVP phase


# Getting started
Install the dq suite on your compute, for example by running the following code in your workspace:

```
pip install dq-suite-amsterdam
```

```
import dq_suite
```

Load your data in dataframes, give them a table_name, and create a list of all dataframes:

```
df = spark.read.csv(csv_path+file_name, header=True, inferSchema=True) #example using csv
df.table_name = "showcase_table"
dfs = [df]
```

- Define 'dfs' as a list of dataframes that require a dq check
- Define 'dq_rules' as a JSON as shown in dq_rules_example.json in this repo
- Define a name for your dq check, in this case "showcase"

```
dq_suite.df_check(dfs, dq_rules, "dpxx_dev", "showcase", spark)
```
# Create dataquality schema and tables (in respective catalog of data team)

for the first time installation create data quality schema and tables from the notebook from repo path scripts/data_quality_tables.sql
- open the notebook, connect to a cluster
- select the catalog of the data team and execute the notebook. It will check if schema is available if not it will create schema and same for tables.

# Export the schema from Unity Catalog to the Input Form
In order to output the schema from Unity Catalog, use the following commands (using the required schema name):

```
schema_output = dq_suite.export_schema('schema_name', spark)
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
The functions can run on Databricks using a Personal Compute Cluster or using a Job Cluster. Using a Shared Compute Cluster will results in an error, as it does not have the permissions that Great Expectations requires.


# Contributing to this library
See the separate [developers readme](src/Readme-dev.md).


# Updates
Version 0.1: Run a DQ check for a dataframe

Version 0.2: Run a DQ check for multiple dataframes

Version 0.3: Refactored I/O

Version 0.4: Added schema validation with Amsterdam Schema per table

Version 0.5: Export schema from Unity Catalog

Version 0.6: The results are written to tables in the "dataquality" schema
