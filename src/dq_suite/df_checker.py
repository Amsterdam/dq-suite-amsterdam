import json
from jsonschema import validate as validate_json

from typing import Dict, Any, Tuple
import pandas as pd

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

from dq_suite.input_validator import validate_dqrules, expand_input
from dq_suite.output_transformations import extract_dq_validatie_data, extract_dq_afwijking_data, create_brontabel, create_bronattribute, create_dqRegel

def df_check(dfs: list, dq_rules: str, check_name: str) -> Tuple[Dict[str, Any], Dict[str, Tuple[Any, Any]], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
    :return: A dictionary of Data Quality results for each DataFrame, 
             along with metadata DataFrames:  brontabel_df, bronattribute_df, dqRegel_df .
             Results contains 'result_dqValidatie' and 'result_dqAfwijking'.
    :rtype: Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]
    """

    results = {}
    name = check_name

    validate_dqrules(dq_rules)
    initial_rule_json = json.loads(dq_rules)
    rule_json = expand_input(initial_rule_json)

    brontabel_df = create_brontabel(rule_json)
    bronattribute_df = create_bronattribute(rule_json)
    dqRegel_df = create_dqRegel(rule_json)

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
        matching_rules = [rule for rule in rule_json["dataframe_parameters"] if rule["table_name"] == df.table_name]

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
            print(f"{df_name} output: ", output)
            for key, value in output.items():
                result = value["validation_result"]
                result_dqValidatie = extract_dq_validatie_data(name, result)
                result_dqAfwijking = extract_dq_afwijking_data(name, result, df, unique_identifier)
                results[df_name] = (result_dqValidatie, result_dqAfwijking)
      
    return  results ,brontabel_df, bronattribute_df, dqRegel_df