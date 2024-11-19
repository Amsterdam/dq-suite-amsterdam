# About dq-suite-amsterdam
This repository aims to be an easy-to-use wrapper for the data quality library [Great Expectations](https://github.com/great-expectations/great_expectations) (GX). All that is needed to get started is an in-memory Spark dataframe and a set of data quality rules - specified in a JSON file [of particular formatting](dq_rules_example.json). 

While the results of all validations are written to a `data_quality` schema in Unity Catalog, users can also choose to get notified via Slack or Microsoft Teams.

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
    validation_name="my_validation_name",
)
```
See the documentation of `dq_suite.validation.run` for what other parameters can be passed.


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
