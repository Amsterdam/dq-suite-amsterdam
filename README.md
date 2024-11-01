# About dq-suite-amsterdam
This repository aims to be an easy-to-use wrapper for the data quality library [Great Expectations](https://github.com/great-expectations/great_expectations) (GX). All that is needed to get started is in-memory Spark dataframe and a set of data quality rules - specified in a JSON file [of particular formatting](dq_rules_example.json). 

While the results of all validations are written to a `data_quality` schema in Unity Catalog, users can also choose to get notified via Slack or Microsoft Teams.

<img src="docs/wip_computer.jpg" width="20%" height="auto">

DISCLAIMER: The package is in MVP phase, so watch your step. 


# Getting started
Following GX, we recommend installing `dq-suite-amsterdam` in a virtual environment. This could be either locally via your IDE, on your compute via a notebook in Databricks, or as part of a workflow. 

1. Run the following command:
```
pip install dq-suite-amsterdam
```

2. Create the `data_quality` schema (and tables) by running the SQL notebook located [here](scripts/data_quality_tables.sql). All it needs is the name of the catalog (and the rights to create a schema within that catalog).

3. Validate your first table. To do so, 
- define `catalog_name` as the name of your catalog
- define `table_name` as the name of the table for which a data quality check is required. This name should also occur in the JSON file
- define `dq_rule_json_path` as a path to a JSON file, formatted in [this](dq_rules_example.json) way
- load the table requiring a data quality check into a Spark dataframe `df` (e.g. via `spark.read.csv` or `spark.read.table`)
- finally, run the following:
```python
import dq_suite

validation_settings_obj = dq_suite.ValidationSettings(spark_session=spark, 
                                                      catalog_name=catalog_name,
                                                      table_name=table_name,
                                                      check_name="name_of_check_goes_here")
dq_suite.run(json_path=dq_rule_json_path, df=df, validation_settings_obj=validation_settings_obj)
```
Note: Looping over multiple data frames may require a redefinition of the `json_path` and `validation_settings` variables. 

See the documentation of `ValidationSettings` for what other parameters can be passed upon intialisation. 


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

- The run_time is defined separately from Great Expectations in df_checker. We plan on fixing it when Great Expectations has documented how to access it from the RunIdentifier object.

# Contributing to this library
See the separate [developers' readme](docs/Readme-dev.md).


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
