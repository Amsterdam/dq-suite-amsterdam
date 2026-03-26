# Input JSON guidelines
This page contains guidelines for creating the json file for dq suite expectations used by `dq-suite` library. 

## Table of contents
- [Team](#team)
- [Dataset](#dataset)
- [Tables](tables)


## team
teamid must be unique in Amsterdam and it can be taken from the standard naming used in databricks as below
- Teamid Example:
dpba, dpbk,dpbr,dpbs,dpbx,dpcr,cpcv.. etc
- teamname and teamdescription are free text, best suiting the team.


```python
{
    "team": {
        "teamid": "dpxx",
        "teamname": "xx team",
        "teamdescription": "xx team"
    },
}
 

```

## dataset

The 'name' must be catalog name of the team and the 'layer' must be the schema name where the table is stored. this is required to make a unique key for the tables defined in the next step. the uniqueness format is dataset_layer_table.

```
    "dataset": {
        "name": "dpso_prd",
        "layer": "Brons"
    }
```

## tables
The table section contains the validation rules per table which can be defined on table or column level. `mask_columns` section should have column names in the table which need to be masked while writing data in validation tables. These column will be masked and hence data privacy concerns can be handled for sensitive data. 
```
"tables": [
        {   
            "unique_identifier": "id",
            "table_name": "well",
            "rules": [
                {
                    "rule_name": "ExpectColumnValuesToBeBetween",
                    "severity" : "fatal",
                    "parameters": {
                            "column": "latitude",
                            "min_value": 6,
                            "max_value": 10000
                        }
                }
            ],
            "mask_columns": [
                "owner"
            ]
        }
]
```

## Add severity level to the Input Form
We added a severity level to the `Input Form` to help with prioritizing failed validation rules based on their criticality.

The severity level is determined using the function:

`get_highest_severity_from_validation_result`
(imported `from src.dq_suite.output_transformations`)

This function extracts the highest severity level from the validation result by checking which failed rules have the most critical severity assigned.

Severity levels (from highest to lowest):

`fatal`: Fails the workflow. Workflow can't continue with this error.

`error`: Does not fail the workflow, but should be fixed A.S.A.P.

`warning`: Does not fail the workflow, but should be fixed eventually.

If no failed expectations match any defined severity, the function returns `ok`.

This function is used within `dq_suite.validation.run_validation`, and its output is included in the final validation result for easier downstream processing and reporting.