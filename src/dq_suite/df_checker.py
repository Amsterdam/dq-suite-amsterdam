from typing import Any, List, Tuple

from great_expectations.checkpoint import Checkpoint
from great_expectations.validator.validator import Validator
from pyspark.sql import DataFrame

from .common import DataQualityRulesDict, Rule, RulesDict, ValidationSettings
from .input_helpers import get_data_quality_rules_dict
from .output_transformations import (
    write_non_validation_tables,
    write_validation_table,
)


def filter_validation_dict_by_table_name(
    validation_dict: DataQualityRulesDict, table_name: str
) -> RulesDict | None:
    for rules_dict in validation_dict["tables"]:
        if rules_dict["table_name"] == table_name:
            # Only one RulesDict per table expected, so return the first match
            return rules_dict
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


def create_action_list(
    validation_settings_obj: ValidationSettings,
) -> List[dict[str, Any]]:
    action_list = list()

    action_list.append(
        {  # TODO/check: do we really have to store the validation results?
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        }
    )

    if validation_settings_obj.send_slack_notification & (
        validation_settings_obj.slack_webhook is not None
    ):
        action_list.append(
            {
                "name": "send_slack_notification",
                "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook": validation_settings_obj.slack_webhook,
                    "notify_on": validation_settings_obj.notify_on,
                    "renderer": {
                        "module_name": "great_expectations.render.renderer.slack_renderer",
                        "class_name": "SlackRenderer",
                    },
                },
            }
        )

    if validation_settings_obj.send_ms_teams_notification & (
        validation_settings_obj.ms_teams_webhook is not None
    ):
        action_list.append(
            {
                "name": "send_ms_teams_notification",
                "action": {
                    "class_name": "MicrosoftTeamsNotificationAction",
                    "microsoft_teams_webhook": validation_settings_obj.ms_teams_webhook,
                    "notify_on": validation_settings_obj.notify_on,
                    "renderer": {
                        "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
                        "class_name": "MicrosoftTeamsRenderer",
                    },
                },
            }
        )

    return action_list


def create_and_run_checkpoint(
    validation_settings_obj: ValidationSettings, batch_request: Any
) -> Any:
    action_list = create_action_list(
        validation_settings_obj=validation_settings_obj
    )
    checkpoint = Checkpoint(
        name=validation_settings_obj.checkpoint_name,
        run_name_template=validation_settings_obj.run_name,
        data_context=validation_settings_obj.data_context,
        batch_request=batch_request,
        expectation_suite_name=validation_settings_obj.expectation_suite_name,
        action_list=action_list,
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
    validation_dict = get_data_quality_rules_dict(file_path=json_path)
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
        dataset_name=validation_dict["dataset"]["name"],
        unique_identifier=rules_dict["unique_identifier"],
    )
