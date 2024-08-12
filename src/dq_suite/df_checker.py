from dataclasses import dataclass
from typing import Any, List, Tuple

from great_expectations import get_context
from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import AbstractDataContext
from great_expectations.validator.validator import Validator
from pyspark.sql import DataFrame, SparkSession

from src.dq_suite.common import (
    DataQualityRulesDict,
    dq_rules_json_string_to_dict, RulesDictList,
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
        data_context_root_dir=data_context_root_dir)


def write_non_validation_tables_to_unity_catalog(
    dq_rules_dict: DataQualityRulesDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    create_brontabel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark_session=spark_session,
    )
    create_bronattribute(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark_session=spark_session,
    )
    create_dqRegel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark_session=spark_session,
    )


def read_data_quality_rules_from_json(file_path: str) -> str:
    with open(file_path, "r") as json_file:
        dq_rules_json_string = json_file.read()
    return dq_rules_json_string


def get_batch_request_and_validator(data_context: AbstractDataContext,
                                    df: DataFrame, check_name: str,
                                    expectation_suite_name: str) -> (
        Tuple)[Any, Validator]:
    dataframe_datasource = data_context.sources.add_or_update_spark(
        name="my_spark_in_memory_datasource_" + check_name
    )

    df_asset = dataframe_datasource.add_dataframe_asset(
        name=check_name, dataframe=df
    )
    batch_request = df_asset.build_batch_request()

    validator = data_context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )

    return batch_request, validator


# TODO: use this as main entry point for library? - after reading a dataframe,
#  setting df.table_name and creating a ValidationSettings object
def run_validation(
    json_path: str, df: DataFrame, validation_settings_obj: ValidationSettings
):
    """
    read_dq_rules from json_path
    
    for table_path in table_paths:
        read_dataframe from table_path
        
        validate_dataframe
    
        write_results_and_metadata to unity catalog
    """
    dq_rules_json_string = read_data_quality_rules_from_json(
        file_path=json_path
    )
    dq_rules_dict = validate_and_load_dqrules(
        dq_rules_json_string=dq_rules_json_string
    )

    # TODO: remove use of list; replace use of json_string with dict; replace
    #  catalog/check/spark parameters with validation_settings object
    validate_dataframes(
        dataframe_list=[df],
        dq_rules_json_string=dq_rules_json_string,
        validation_settings_obj=validation_settings_obj,
    )

    write_non_validation_tables_to_unity_catalog(
        dq_rules_dict=dq_rules_dict,
        validation_settings_obj=validation_settings_obj
    )


def validate_dataframes(
    dataframe_list: List[DataFrame],
    dq_rules_json_string: str,
    catalog_name: str,
    check_name: str,
    spark_session: SparkSession,
) -> None:
    """
    Function takes DataFrame instances with specified Data Quality rules.
    and returns a JSON string with the DQ results with different dataframes
    in results dict, and returns different dataframe_list as specified using
    Data Quality rules

    :param dataframe_list: A list of DataFrame instances to process.
    :param dq_rules_json_string: JSON string containing the Data Quality
    rules to be evaluated.
    :param catalog_name: [explanation goes here]
    :param check_name: Name of the run for reference purposes.
    :param spark_session: [explanation goes here]
    """
    # TODO/check: use file path instead of JSON string?
    # dq_rules_json_string = read_data_quality_rules_from_json(
    #     file_path=json_file_path)

    dq_rules_dict = dq_rules_json_string_to_dict(
        dq_rules_json_string=dq_rules_json_string
    )

    write_non_validation_tables_to_unity_catalog(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark_session=spark_session,
    )

    data_context = get_data_context()
    expectation_suite_name = check_name + "_exp_suite"

    data_context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )

    checkpoint_name = check_name + "_checkpoint"
    run_name_template = "%Y%m%d-%H%M%S-" + check_name + "-template"

    for df in dataframe_list:
        batch_request, validator = get_batch_request_and_validator(
            data_context=data_context, df=df, check_name=check_name,
            expectation_suite_name=expectation_suite_name)

        # to compare table_name in dq_rules and given table_names by data teams
        matching_rules: RulesDictList = [
            rules_dict
            for rules_dict in dq_rules_dict["tables"]
            if rules_dict["table_name"] == df.table_name
        ]

        if not matching_rules:
            continue

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
                data_context=data_context,
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                ],
            )

            data_context.add_or_update_checkpoint(checkpoint=checkpoint)
            checkpoint_result = checkpoint.run()
            output = checkpoint_result["run_results"]
            for key, value in output.items():
                result = value["validation_result"]
                extract_dq_validatie_data(df_name, result, catalog_name,
                                          spark_session)
                extract_dq_afwijking_data(
                    df_name, result, df, unique_identifier, catalog_name,
                    spark_session)
