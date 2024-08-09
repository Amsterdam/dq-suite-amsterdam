from typing import List

from great_expectations import get_context
from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import AbstractDataContext
from pyspark.sql import DataFrame, SparkSession

from src.dq_suite.input_helpers import (
    DataQualityRulesDict,
    expand_input,
    generate_dq_rules_from_schema,
    validate_and_load_dqrules,
)
from src.dq_suite.output_transformations import (
    create_bronattribute,
    create_brontabel,
    create_dqRegel,
    extract_dq_afwijking_data,
    extract_dq_validatie_data,
)


def get_data_context(
    context_root_dir: str = "/dbfs/great_expectations/",
) -> AbstractDataContext:
    return get_context(context_root_dir=context_root_dir)


def write_non_validation_tables_to_unity_catalog(
    dq_rules_dict: DataQualityRulesDict,
    catalog_name: str,
    spark_session: SparkSession,
) -> None:
    create_brontabel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark=spark_session,
    )
    create_bronattribute(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark=spark_session,
    )
    create_dqRegel(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark=spark_session,
    )


def validate_dataframes(
    dataframe_list: List[DataFrame],
    dq_rules: str,
    catalog_name: str,
    check_name: str,
    spark: SparkSession,
) -> None:
    """
    Function takes DataFrame instances with specified Data Quality rules.
    and returns a JSON string with the DQ results with different dataframes
    in results dict, and returns different dataframe_list as specified using Data
    Quality rules

    :param dataframe_list: A list of DataFrame instances to process.
    :param dq_rules: JSON string containing the Data Quality rules to be
    evaluated.
    :param catalog_name: [explanation goes here]
    :param check_name: Name of the run for reference purposes.
    :param spark: [explanation goes here]
    """
    # TODO: refactor into subsequent function
    initial_rule_json = validate_and_load_dqrules(dq_rules=dq_rules)  
    dq_rules_dict = expand_input(rule_json=initial_rule_json)

    dq_rules_dict = generate_dq_rules_from_schema(dq_rules_dict=dq_rules_dict)

    write_non_validation_tables_to_unity_catalog(
        dq_rules_dict=dq_rules_dict,
        catalog_name=catalog_name,
        spark_session=spark,
    )

    data_context = get_data_context()

    for df in dataframe_list:
        dataframe_datasource = data_context.sources.add_or_update_spark(
            name="my_spark_in_memory_datasource_" + check_name
        )

        df_asset = dataframe_datasource.add_dataframe_asset(
            name=check_name, dataframe=df
        )
        batch_request = df_asset.build_batch_request()

        expectation_suite_name = check_name + "_exp_suite"
        checkpoint_name = check_name + "_checkpoint"

        data_context.add_or_update_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )

        validator = data_context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
        )

        # to compare table_name in dq_rules and given table_names by data teams
        matching_rules = [
            rule
            for rule in dq_rules_dict["tables"]
            if rule["table_name"] == df.table_name
        ]

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

            checkpoint = Checkpoint(
                name=checkpoint_name,
                run_name_template="%Y%m%d-%H%M%S-" + check_name + "-template",
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
                extract_dq_validatie_data(df_name, result, catalog_name, spark)
                extract_dq_afwijking_data(
                    df_name, result, df, unique_identifier, catalog_name, spark
                )

    return
