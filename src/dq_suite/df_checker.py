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
    Rule,
    RulesDict,
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
    table_name: str
    check_name: str
    data_context_root_dir: str = "/dbfs/great_expectations/"
    data_context: AbstractDataContext | None = None
    expectation_suite_name: str | None = None
    checkpoint_name: str | None = None
    run_name: str | None = None

    def initialise_or_update_attributes(self):
        self._set_data_context()

        # TODO/check: do we want to allow for custom names?
        self._set_expectation_suite_name()
        self._set_checkpoint_name()
        self._set_run_name()

        # Finally, apply the (new) suite name to the data context
        self.data_context.add_or_update_expectation_suite(
            expectation_suite_name=self.expectation_suite_name
        )

    def _set_data_context(self):
        self.data_context = get_data_context(
            data_context_root_dir=self.data_context_root_dir
        )

    def _set_expectation_suite_name(self):
        self.expectation_suite_name = f"{self.check_name}_expectation_suite"

    def _set_checkpoint_name(self):
        self.checkpoint_name = f"{self.check_name}_checkpoint"

    def _set_run_name(self):
        self.run_name = f"%Y%m%d-%H%M%S-{self.check_name}"


def write_non_validation_tables(
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


def write_validation_table(
    validation_output: Any,
    validation_settings_obj: ValidationSettings,
    df: DataFrame,
    unique_identifier: str,
):
    for results in validation_output.values():
        result = results["validation_result"]
        extract_dq_validatie_data(
            validation_settings_obj.table_name,
            result,
            validation_settings_obj.catalog_name,
            validation_settings_obj.spark_session,
        )
        extract_dq_afwijking_data(
            validation_settings_obj.table_name,
            result,
            df,
            unique_identifier,
            validation_settings_obj.catalog_name,
            validation_settings_obj.spark_session,
        )


def read_data_quality_rules_from_json(file_path: str) -> str:
    with open(file_path, "r") as json_file:
        dq_rules_json_string = json_file.read()
    return dq_rules_json_string


def validate_and_load_dqrules(dq_rules_json_string: str) -> Any | None:
    """
    Deserializes a JSON document in string format, and prints one or more error
    messages in case a JSONDecodeError is raised.

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
            print("Rules' Value is missing in the JSON.")

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
    validation_dict = validate_and_load_dqrules(
        dq_rules_json_string=dq_rules_json_string
    )
    return validation_dict


def filter_validation_dict_by_table_name(
    validation_dict: DataQualityRulesDict, table_name: str
) -> RulesDict | None:
    for rules_dict in validation_dict["tables"]:
        if rules_dict["table_name"] == table_name:
            return rules_dict  # Return only the first match
    return None


def get_batch_request_and_validator(
    df: DataFrame,
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
        expectation_suite_name=validation_settings_obj.expectation_suite_name,
    )

    return batch_request, validator


def create_and_run_checkpoint(
    validation_settings_obj: ValidationSettings, batch_request: Any
) -> Any:
    checkpoint = Checkpoint(
        name=validation_settings_obj.checkpoint_name,
        run_name_template=validation_settings_obj.run_name,
        data_context=validation_settings_obj.data_context,
        batch_request=batch_request,
        expectation_suite_name=validation_settings_obj.expectation_suite_name,
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },  # TODO: add more options via parameters, e.g. Slack output
        ],
    )

    validation_settings_obj.data_context.add_or_update_checkpoint(
        checkpoint=checkpoint
    )
    checkpoint_result = checkpoint.run()
    return checkpoint_result["run_results"]


def create_and_configure_expectations(
    validation_rules_list: List[Rule], validator: Validator
) -> None:
    for validation_rule in validation_rules_list:
        # Get the name of expectation as defined by GX
        gx_expectation_name = validation_rule["rule_name"]

        # Get the actual expectation as defined by GX
        gx_expectation = getattr(validator, gx_expectation_name)
        for validation_parameter_dict in validation_rule["parameters"]:
            kwargs = {}
            for par_name, par_value in validation_parameter_dict.items():
                kwargs[par_name] = par_value
            gx_expectation(**kwargs)

    validator.save_expectation_suite(discard_failed_expectations=False)


def validate(
    df: DataFrame,
    rules_dict: RulesDict,
    validation_settings_obj: ValidationSettings,
) -> Any:
    """
    [explanation goes here]

    :param df: A list of DataFrame instances to process.
    :param rules_dict: a RulesDict object containing the
    data quality rules to be evaluated.
    :param validation_settings_obj: [explanation goes here]
    """
    # Make sure all attributes are aligned before validating
    validation_settings_obj.initialise_or_update_attributes()

    batch_request, validator = get_batch_request_and_validator(
        df=df,
        validation_settings_obj=validation_settings_obj,
    )

    create_and_configure_expectations(
        validation_rules_list=rules_dict["rules"], validator=validator
    )

    checkpoint_output = create_and_run_checkpoint(
        validation_settings_obj=validation_settings_obj,
        batch_request=batch_request,
    )

    return checkpoint_output


def run(
    json_path: str, df: DataFrame, validation_settings_obj: ValidationSettings
) -> None:
    if not hasattr(df, "table_name"):
        df.table_name = validation_settings_obj.table_name

    # 1) extract the data quality rules to be applied...
    validation_dict = get_validation_dict(file_path=json_path)
    rules_dict = filter_validation_dict_by_table_name(
        validation_dict=validation_dict,
        table_name=validation_settings_obj.table_name,
    )
    if rules_dict is None:
        print(
            f"No validations found for table_name "
            f"'{validation_settings_obj.table_name}' in JSON file at '"
            f"{json_path}'."
        )
        return

    # 2) perform the validation on the dataframe
    validation_output = validate(
        df=df,
        rules_dict=rules_dict,
        validation_settings_obj=validation_settings_obj,
    )

    # 3) write results to unity catalog
    write_non_validation_tables(
        dq_rules_dict=validation_dict,
        validation_settings_obj=validation_settings_obj,
    )
    write_validation_table(
        validation_output=validation_output,
        validation_settings_obj=validation_settings_obj,
        df=df,
        unique_identifier=rules_dict["unique_identifier"],
    )
