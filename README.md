# Introduction 
This repository contains functions that will ease the use of Great Expectations. Users can input data and data quality rules and get results in return.

DISCLAIMER: Repo is in PoC phase

DISCLAIMER: The functions can run on Databricks using a Personal Compute Cluster


# Getting Started
Run the following code in your workspace:

```
pip install great_expectations
```

```
pip install dq-suite-amsterdam
```

```
import dq_suite
```

- Define 'dfs' as a list of dataframes that require a dq check
- Define 'dq_rules' as a JSON as shown in dq_rules_example.json in this repo

```
results, brontabel_df, bronattribute_df, dqRegel_df = dq_suite.df_check(dfs, dq_rules, "showcase")
```


# Updates
version = "0.1.0" :
dq_rules_example.json is updated.
Added:
"dataframe_parameters": {
        "unique_identifier": "id"
    }

version = "0.2.0" :
dq_rules_example.json is updated.
Added for each tables:
{
    "dataframe_parameters": [
        {
            "unique_identifier": "id",
            "table_name": "well",
            "rules": [ 
                {
                    "rule_name": "expect_column_values_to_be_between",
                    "parameters": [
                        {
                            "column": "latitude",
                            "min_value": 6,
                            "max_value": 10000
                        }
                    ]
                }
            ]
        },
        ....
