# Introduction 
DISCLAIMER: Repo is in PoC phase
DISCLAIMER: The functions can run on Databricks using a Personal Compute Cluster

This repository contains functions that will ease the use of Great Expectations. Users can input data and data quality rules and get rules in return.


# Getting Started
Prerequisites:

Run the following code in your workspace:

pip install great_expectations

When working in Databricks you can clone this repo to Databricks Repos. Then you can access it in your workspace using:

import sys
sys.path.append("/Workspace/Repos/{user}/{repo_name}")
from {file} import {function}

Parameter examples:
user: j.cruijff@amsterdam.nl
repo_name: dq_repo
file: df_checker
function: df_check

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