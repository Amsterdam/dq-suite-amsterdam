import json
from jsonschema import validate as validate_json

from databricks.sdk.runtime import *

from pyspark.sql import DataFrame

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

from dq_suite.input_validator import validate_dqrules
from dq_suite.output_transformations import extract_dq_validatie_data
from dq_suite.output_transformations import extract_dq_afwijking_data


def df_check(df: DataFrame, dq_rules: str, check_name: str) -> str:
    """
    Function takes a DataFrame instance and returns a JSON string with the DQ results.

    :param df: A DataFrame instance to process
    :type df: DataFrame
    :param dq_rules: A JSON string containing the Data Quality rules to be evaluated
    :type dq_rules: str
    :param check_name: Name of the run for reference purposes
    :type check_name: str
    :return: Two tables df result_dqValidatie - result_dqAfwijking with the DQ results, parsed from the GX output
    :rtype: df.
    """
    name = check_name
    validate_dqrules(dq_rules)
    rule_json = json.loads(dq_rules)
  
    # Configure the Great Expectations context
    context_root_dir = "/dbfs/great_expectations/"
    context = gx.get_context(context_root_dir=context_root_dir)

    dataframe_datasource = context.sources.add_or_update_spark(
        name="my_spark_in_memory_datasource_" + name,
    )

    # GX Structures
    df_asset = dataframe_datasource.add_dataframe_asset(name=name, dataframe=df)
    batch_request = df_asset.build_batch_request()

    expectation_suite_name = name + "_exp_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )

    # DQ rules
    # This section converts the DQ_rules input into expectations that Great Expectations understands
    for rule in rule_json["rules"]:
        check = getattr(validator, rule["rule_name"])
        
        for param_set in rule["parameters"]:
            kwargs = {}
            for param in param_set.keys():
                kwargs[param] = param_set[param]
            check(**kwargs)
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    
    # Save output
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

    # Parse output
    output = checkpoint_result["run_results"]
    
    for key in output.keys():
        result = output[key]["validation_result"]
        result_dqValidatie = extract_dq_validatie_data(name,result)
        result_dqAfwijking = extract_dq_afwijking_data(name,result)

    return result_dqValidatie, result_dqAfwijking
