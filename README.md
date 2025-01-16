# About dq-suite-amsterdam
This repository aims to be an easy-to-use wrapper for the data quality library [Great Expectations](https://github.com/great-expectations/great_expectations) (GX). All that is needed to get started is an in-memory Spark dataframe and a set of data quality rules - specified in a JSON file [of particular formatting](dq_rules_example.json). 

By default, none of the validation results are written to Unity Catalog. Alternatively, one could allow for writing to a `data_quality` schema in UC, which one has to create once per catalog via [this notebook](scripts/data_quality_tables.sql). Additionally, users can choose to get notified via Slack or Microsoft Teams.

<img src="docs/wip_computer.jpg" width="20%" height="auto">

DISCLAIMER: The package is in MVP phase, so watch your step. 


## How to contribute
Want to help out? Great! Feel free to create a pull request addressing one of the open [issues](https://github.com/Amsterdam/dq-suite-amsterdam/issues). Some notes for developers are located [here](docs/Readme-dev.md).

Found a bug, or need a new feature? Add a new issue describing what you need. 


# Getting started
Following GX, we recommend installing `dq-suite-amsterdam` in a virtual environment. This could be either locally via your IDE, on your compute via a notebook in Databricks, or as part of a workflow. 

1. Run the following command:
```
pip install dq-suite-amsterdam
```

2. Create the `data_quality` schema (and tables all results will be written to) by running the SQL notebook located [here](scripts/data_quality_tables.sql). All it needs is the name of the catalog - and the rights to create a schema within that catalog :)


3. Get ready to validate your first table. To do so, define
- `dq_rule_json_path` as a path to a JSON file, formatted in [this](dq_rules_example.json) way
- `df` as a Spark dataframe containing the table that needs to be validated (e.g. via `spark.read.csv` or `spark.read.table`)
- `spark` as a SparkSession object (in Databricks notebooks, this is by default called `spark`)
- `catalog_name` as the name of your catalog ('dpxx_dev' or 'dpxx_prd')
- `table_name` as the name of the table for which a data quality check is required. This name should also occur in the JSON file at `dq_rule_json_path`



4. Finally, perform the validation by running
```python
from dq_suite.validation import run_validation

run_validation(
    json_path=dq_rule_json_path,
    df=df, 
    spark_session=spark,
    catalog_name=catalog_name,
    table_name=table_name,
)
```
See the documentation of `dq_suite.validation.run_validation` for what other parameters can be passed.


# Other functionalities
## Export the schema from Unity Catalog to the Input Form
In order to output the schema from Unity Catalog, use the following commands (using the required schema name):
```
schema_output = dq_suite.schema_to_json_string('schema_name', spark, *table)
print(schema_output)
```
Copy the string to the Input Form to quickly ingest the schema in Excel. The "table" parameter is optional, it gives more granular results.
## Validate the schema of a table
It is possible to validate the schema of an entire table to a schema definition from Amsterdam Schema in one go. This is done by adding two fields to the "dq_rules" JSON when describing the table (See: https://github.com/Amsterdam/dq-suite-amsterdam/blob/main/dq_rules_example.json). 
You will need:
- validate_table_schema: the id field of the table from Amsterdam Schema
- validate_table_schema_url: the url of the table or dataset from Amsterdam Schema
The schema definition is converted into column level expectations (ExpectColumnValuesToBeOfType) on run time.


# Known exceptions / issues
- The functions can run on Databricks using a Personal Compute Cluster or using a Job Cluster. 
Using a Shared Compute Cluster will result in an error, as it does not have the permissions that Great Expectations requires.

- Since this project requires Python >= 3.10, the use of Databricks Runtime (DBR) >= 13.3 is needed 
([click](https://docs.databricks.com/en/release-notes/runtime/13.3lts.html#system-environment)). 
Older versions of DBR will result in errors upon install of the `dq-suite-amsterdam` library.

- At time of writing (late Aug 2024), Great Expectations v1.0.0 has just been released, and is not (yet) compatible with Python 3.12. Hence, make sure you are using the correct version of Python as interpreter for your project.

- The `run_time` value is defined separately from Great Expectations in `validation.py`. We plan on fixing this when Great Expectations has documented how to access it from the RunIdentifier object.


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

Version 0.11: Stability and testability improvements
