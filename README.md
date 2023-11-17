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
