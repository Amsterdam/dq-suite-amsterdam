import json
from jsonschema import validate as validate_json
from typing import Dict, Any, Tuple

from pyspark.sql import SparkSession
import pandas as pd
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

from dq_suite.input_helpers import validate_dqrules, expand_input, export_schema, generate_dq_rules_from_schema, fetch_schema_from_github
from dq_suite.output_transformations import extract_dq_validatie_data, extract_dq_afwijking_data, create_brontabel, create_bronattribute, create_dqRegel

def df_check(dfs: list, dq_rules: str, catalog_name: str, check_name: str, spark: SparkSession):
    """
    Function takes DataFrame instances with specified Data Quality rules. 
    and returns a JSON string with the DQ results with different dataframes in results dict, 
    and returns different dfs as specified using Data Quality rules
    
    :param dfs: A list of DataFrame instances to process.
    :type dfs: list[DataFrame]
    :param dq_rules: JSON string containing the Data Quality rules to be evaluated.
    :type dq_rules: str
    :param check_name: Name of the run for reference purposes.
    :type check_name: str
    """

    results = {}
    name = check_name

    validate_dqrules(dq_rules)
    initial_rule_json = json.loads(dq_rules)
    rule_json = expand_input(initial_rule_json)

    # Generate DQ rules from schema
    schema = fetch_schema_from_github(rule_json) 
    rule_json = generate_dq_rules_from_schema(rule_json, schema)

    create_brontabel(rule_json, catalog_name, spark)
    create_bronattribute(rule_json, catalog_name, spark)
    create_dqRegel(rule_json, catalog_name, spark)

    for df in dfs:
        context_root_dir = "/dbfs/great_expectations/"
        context = gx.get_context(context_root_dir=context_root_dir)
 
        dataframe_datasource = context.sources.add_or_update_spark(name="my_spark_in_memory_datasource_" + name)
 
        df_asset = dataframe_datasource.add_dataframe_asset(name=name, dataframe=df)
        batch_request = df_asset.build_batch_request()
 
        expectation_suite_name = name + "_exp_suite"
        context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
 
        validator = context.get_validator(batch_request=batch_request, expectation_suite_name=expectation_suite_name)

        # to compare table_name in dq_rules and given table_names by data teams
        matching_rules = [rule for rule in rule_json["tables"] if rule["table_name"] == df.table_name]

        if not matching_rules:
            continue
 
        for rule in matching_rules:
            df_name = rule["table_name"]
            unique_identifier = rule["unique_identifier"]
            for rule_param in rule["rules"]:
                check = getattr(validator, rule_param["rule_name"])
                for param_set in rule_param["parameters"]:
                    kwargs = {}
                    for param in param_set.keys():
                        kwargs[param] = param_set[param]
                    check(**kwargs)
 
            validator.save_expectation_suite(discard_failed_expectations=False)
 
            my_checkpoint_name = name + "_checkpoint"
 
            checkpoint = Checkpoint(
                name=my_checkpoint_name,
                run_name_template="%Y%m%d-%H%M%S-" + name + "-template",
                data_context=context,
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                ],
            )
 
            context.add_or_update_checkpoint(checkpoint=checkpoint)
            checkpoint_result = checkpoint.run()
            output = checkpoint_result["run_results"]
            for key, value in output.items():
                result = value["validation_result"]
                extract_dq_validatie_data(df_name, result, catalog_name, spark)
                extract_dq_afwijking_data(df_name, result, df, unique_identifier, catalog_name, spark)

    return
