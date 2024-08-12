import json
from dataclasses import dataclass
from typing import Any, List, Tuple

from great_expectations import get_context
from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import AbstractDataContext
from great_expectations.validator.validator import Validator
from pyspark.sql import DataFrame, SparkSession

from src.dq_suite.common import (
    DataQualityRulesDict,
    RulesDictList,
    generate_dq_rules_from_schema,
)
from src.dq_suite.output_transformations import (
    create_bronattribute,
    create_brontabel,
    create_dqRegel,
    extract_dq_afwijking_data,
    extract_dq_validatie_data,
)


def get_data_context(
    data_context_root_dir: str = "/dbfs/great_expectations/",
) -> AbstractDataContext:
    return get_context(context_root_dir=data_context_root_dir)


@dataclass()
class ValidationSettings:
    spark_session: SparkSession
    catalog_name: str
    check_name: str
    data_context_root_dir: str = "/dbfs/great_expectations/"
    data_context: AbstractDataContext = get_data_context(
        data_context_root_dir=data_context_root_dir
    )


def write_non_validation_tables_to_unity_catalog(
    dq_rules_dict: DataQualityRulesDict,
    validation_settings_obj: ValidationSettings,
) -> None:
    create_brontabel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
    create_bronattribute(
        dq_rules_dict=dq_rules_dict,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )
    create_dqRegel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=validation_settings_obj.catalog_name,
        spark_session=validation_settings_obj.spark_session,
    )


def read_data_quality_rules_from_json(file_path: str) -> str:
    with open(file_path, "r") as json_file:
        dq_rules_json_string = json_file.read()
    return dq_rules_json_string


def validate_and_load_dqrules(dq_rules_json_string: str) -> Any | None:
    """
    Function validates the input JSON

    :param dq_rules_json_string: A JSON string with all DQ configuration.
    """

    try:
        return json.loads(dq_rules_json_string)

    except json.JSONDecodeError as e:
        error_message = str(e)
        print(f"Data quality check failed: {error_message}")
        if "Invalid control character at:" in error_message:
            print("Quota is missing in the JSON.")
        if "Expecting ',' delimiter:" in error_message:
            print(
                "Square brackets, Comma or curly brackets can be missing in "
                "the JSON."
            )
        if "Expecting ':' delimiter:" in error_message:
            print("Colon is missing in the JSON.")
        if "Expecting value:" in error_message:
            print("Rules's Value is missing in the JSON.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def dq_rules_json_string_to_dict(
    dq_rules_json_string: str,
) -> DataQualityRulesDict:
    """
    Function adds a mandatory line in case of a conditional rule

    :param dq_rules_json_string: A JSON string with all DQ configuration.
    :return: rule_json: A dictionary with all DQ configuration.
    """
    dq_rules_dict: DataQualityRulesDict = validate_and_load_dqrules(
        dq_rules_json_string=dq_rules_json_string
    )

    for table in dq_rules_dict["tables"]:
        for rule in table["rules"]:
            for parameter in rule["parameters"]:
                if "row_condition" in parameter:
                    #  GX requires this statement for conditional rules when
                    #  using spark
                    parameter[
                        "condition_parser"
                    ] = "great_expectations__experimental__"

    return generate_dq_rules_from_schema(dq_rules_dict=dq_rules_dict)


def get_validation_dict(file_path: str) -> DataQualityRulesDict:
    dq_rules_json_string = read_data_quality_rules_from_json(
        file_path=file_path
    )
    dq_rules_dict = validate_and_load_dqrules(
        dq_rules_json_string=dq_rules_json_string
    )
    return dq_rules_dict


def get_batch_request_and_validator(
    df: DataFrame,
    expectation_suite_name: str,
    validation_settings_obj: ValidationSettings,
) -> Tuple[Any, Validator]:
    dataframe_datasource = (
        validation_settings_obj.data_context.sources.add_or_update_spark(
            name="my_spark_in_memory_datasource_"
            + validation_settings_obj.check_name
        )
    )

    df_asset = dataframe_datasource.add_dataframe_asset(
        name=validation_settings_obj.check_name, dataframe=df
    )
    batch_request = df_asset.build_batch_request()

    validator = validation_settings_obj.data_context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )

    return batch_request, validator


# TODO: use this as main entry point for library? - after reading a dataframe,
#  setting df.table_name and creating a ValidationSettings object
def run_validation(
    json_path: str, df: DataFrame, validation_settings_obj: ValidationSettings
) -> None:
    """
    read_dq_rules from json_path

    read_dataframe from table_path

    validate_dataframe

    write_results_and_metadata to unity catalog
    """
    dq_rules_dict = get_validation_dict(file_path=json_path)

    validate(
        df=df,
        dq_rules_dict=dq_rules_dict,
        validation_settings_obj=validation_settings_obj,
    )

    write_non_validation_tables_to_unity_catalog(
        dq_rules_dict=dq_rules_dict,
        validation_settings_obj=validation_settings_obj,
    )


def validate(
    df: DataFrame,
    dq_rules_dict: DataQualityRulesDict,
    validation_settings_obj: ValidationSettings,
) -> None:
    """
    [explanation goes here]

    :param df: A list of DataFrame instances to process.
    :param dq_rules_dict: a DataQualityRulesDict object containing the data
    quality rules to be evaluated.
    :param validation_settings_obj: [explanation goes here]
    """

    write_non_validation_tables_to_unity_catalog(
        dq_rules_dict=dq_rules_dict,
        validation_settings_obj=validation_settings_obj,
    )

    expectation_suite_name = (
        validation_settings_obj.check_name + "_expectation_suite"
    )

    validation_settings_obj.data_context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )

    checkpoint_name = validation_settings_obj.check_name + "_checkpoint"
    run_name_template = (
        "%Y%m%d-%H%M%S-" + validation_settings_obj.check_name + "-template"
    )

    ############### Previous start of loop over list of dataframes
    batch_request, validator = get_batch_request_and_validator(
        df=df,
        expectation_suite_name=expectation_suite_name,
        validation_settings_obj=validation_settings_obj,
    )

    # to compare table_name in dq_rules and given table_names by data teams
    matching_rules: RulesDictList = [
        rules_dict
        for rules_dict in dq_rules_dict["tables"]
        if rules_dict["table_name"] == df.table_name
    ]

    if not matching_rules:
        return

    for rules_dict in matching_rules:
        df_name = rules_dict["table_name"]
        unique_identifier = rules_dict["unique_identifier"]
        for rule_param in rules_dict["rules"]:
            check = getattr(validator, rule_param["rule_name"])
            for param_set in rule_param["parameters"]:
                kwargs = {}
                for param in param_set.keys():
                    kwargs[param] = param_set[param]
                check(**kwargs)

        validator.save_expectation_suite(discard_failed_expectations=False)

        checkpoint = Checkpoint(
            name=checkpoint_name,
            run_name_template=run_name_template,
            data_context=validation_settings_obj.data_context,
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
            ],
        )

        validation_settings_obj.data_context.add_or_update_checkpoint(
            checkpoint=checkpoint
        )
        checkpoint_result = checkpoint.run()
        output = checkpoint_result["run_results"]
        for key, value in output.items():
            result = value["validation_result"]
            extract_dq_validatie_data(
                df_name,
                result,
                validation_settings_obj.catalog_name,
                validation_settings_obj.spark_session,
            )
            extract_dq_afwijking_data(
                df_name,
                result,
                df,
                unique_identifier,
                validation_settings_obj.catalog_name,
                validation_settings_obj.spark_session,
            )
