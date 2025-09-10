# About dq-suite-amsterdam
This repository aims to be an easy-to-use wrapper for the data quality library [Great Expectations](https://github.com/great-expectations/great_expectations) (GX). All that is needed to get started is an in-memory Spark dataframe and a set of data quality rules - specified in a JSON file [of particular formatting](dq_rules_example.json). 

By default, all the validation results are written to Unity Catalog. Alternatively, one could disallow writing to a `data_quality` schema in UC, which one has to create once per catalog via [this notebook](scripts/data_quality_tables.sql). Additionally, users can choose to get notified via Slack or Microsoft Teams.

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



4. Finally, perform the validation by running (*note*: the library is imported as `dq_suite`, not as `dq_suite_amsterdam`!)

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


**Profiling**

Profiling is the process of analyzing a dataset to understand its structure, patterns, and data quality characteristics (such as completeness, uniqueness, or value distributions). 

The profiling functionality in dq_suite generates profiling results and automatically produces a rules.json file, which can be used as input for the validation—making it easier to gain insights and validate data quality.
1. Run the following command:
```
pip install dq-suite-amsterdam
```
2. Get ready to validate your first table. To do so, define
- `df` as a Panda dataframe containing the table that needs to be validated (e.g. via `pd.read_csv`)
- `generate_rules` as a Boolean to generate dq_rule_json. Set to False if you only want profiling without rule generation
- `spark` as a SparkSession object (in Databricks notebooks, this is by default called `spark`)
- `dq_rule_json_path` as a path to a JSON file, wil be formatted in [this](src/dq_suite/profile/dq_rules_example_from_profiling.json) way after running profiling function
- `dataset_name` as the name of the table for which a data quality check is required. This name will be placed in the JSON file at `dq_rule_json_path`
- `table_name` as the name of the table for which a data quality check is required. This name will be placed in the JSON file at `dq_rule_json_path`
3. Finally, perform the profiling by running 
```python
from dq_suite.profile.profile import profile_and_create_rules

profile_and_create_rules(
    df=df,
    dataset_name=dataset_name,
    table_name=table_name,
    spark_session=spark,
    generate_rules=True,
    rule_path=dq_rule_json_path
)
```

**Result of profiling**

Profiling result will be created in HTML view.
The rule.json file will be created at the specified path.(if you set `generate_rules=True`)
You can edit this file to refine the rules according to your data validation needs.
The JSON rule file can then be used as input for dq_suite validation.

For further documentation, see:
- [other functionalities](docs/Readme-other.md)
- [notes for developers](docs/Readme-dev.md)
- [notes for data engineers at Gemeente Amsterdam](https://dev.azure.com/CloudCompetenceCenter/Dataplatform%20en%20Data%20organisatie/_git/vakgroep_data_engineering?path=/docs/03_knowledge_bank/topics/data_quality/data_quality.md&_a=preview) (in Dutch, employees only)


# Known exceptions / issues
- The functions can run on Databricks using a Personal Compute Cluster or using a Job Cluster. 
Using a Shared Compute Cluster will result in an error, as it does not have the permissions that Great Expectations requires.

- Since this project requires Python >= 3.10, the use of Databricks Runtime (DBR) >= 13.3 is needed 
([click](https://docs.databricks.com/en/release-notes/runtime/13.3lts.html#system-environment)). 
Older versions of DBR will result in errors upon install of the `dq-suite-amsterdam` library.

- At time of writing (late Aug 2024), Great Expectations v1.0.0 has just been released, and is not (yet) compatible with Python 3.12. Hence, make sure you are using the correct version of Python as interpreter for your project.

- The `run_time` value is defined separately from Great Expectations in `validation.py`. We plan on fixing this when Great Expectations has documented how to access it from the RunIdentifier object.

- Profiling rules/Rule condition logic
 
Current profiling-based rule conditions are placeholders and should be defined and validated by the data teams to ensure they are generic and reusable.
