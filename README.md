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
results, brontabel_df, bronattribute_df, dqRegel_df = dq_suite.df_check(dfs, dq_rules, "showcase")
```


# Known exceptions
The functions can run on Databricks using a Personal Compute Cluster or using a Job Cluster. Using a Shared Compute Cluster will results in an error, as it does not have the permissions that Great Expectations requires.


# Updates
Version 0.1: Run a DQ check for a dataframe

Version 0.2: Run a DQ check for multiple dataframes
