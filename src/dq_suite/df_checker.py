from typing import List

import great_expectations
import humps
from great_expectations import Checkpoint, ValidationDefinition
from great_expectations.checkpoint.actions import CheckpointAction
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.exceptions import DataContextError
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


def get_or_add_validation_definition(
    validation_settings_obj: ValidationSettings,
) -> ValidationDefinition:
    dataframe_datasource = (
        validation_settings_obj.data_context.data_sources.add_or_update_spark(
            name=f"spark_datasource_" f"{validation_settings_obj.check_name}"
        )
    )

    df_asset = dataframe_datasource.add_dataframe_asset(
        name=validation_settings_obj.check_name
    )
    batch_definition = df_asset.add_batch_definition_whole_dataframe(
        name=f"{validation_settings_obj.check_name}_batch_definition"
    )

    validation_definition_name = (
        f"{validation_settings_obj.check_name}" f"_validation_definition"
    )
    try:
        validation_definition = (
            validation_settings_obj.data_context.validation_definitions.get(
                name=validation_definition_name
            )
        )
    except DataContextError:
        validation_definition = ValidationDefinition(
            name=validation_definition_name,
            data=batch_definition,
            suite=validation_settings_obj.data_context.suites.get(
                validation_settings_obj.expectation_suite_name
            ),
        )  # Note: a validation definition combines data with a suite of
        # expectations
        validation_definition = (
            validation_settings_obj.data_context.validation_definitions.add(
                validation=validation_definition
            )
        )

    return validation_definition


def create_action_list(
    validation_settings_obj: ValidationSettings,
) -> List[CheckpointAction]:
    action_list = list()

    if validation_settings_obj.send_slack_notification & (
        validation_settings_obj.slack_webhook is not None
    ):
        action_list.append(
            great_expectations.checkpoint.SlackNotificationAction(
                name="send_slack_notification",
                slack_webhook=validation_settings_obj.slack_webhook,
                notify_on=validation_settings_obj.notify_on,
                renderer={
                    "module_name": "great_expectations.render.renderer.slack_renderer",
                    "class_name": "SlackRenderer",
                },
            )
        )

    if validation_settings_obj.send_ms_teams_notification & (
        validation_settings_obj.ms_teams_webhook is not None
    ):
        action_list.append(
            great_expectations.checkpoint.MicrosoftTeamsNotificationAction(
                name="send_ms_teams_notification",
                microsoft_teams_webhook=validation_settings_obj.ms_teams_webhook,
                notify_on=validation_settings_obj.notify_on,
                renderer={
                    "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
                    "class_name": "MicrosoftTeamsRenderer",
                },
            )
        )

    return action_list


def get_or_add_checkpoint(
    validation_settings_obj: ValidationSettings,
    validation_definition: ValidationDefinition,
) -> Checkpoint:
    try:
        checkpoint = validation_settings_obj.data_context.checkpoints.get(
            name=validation_settings_obj.checkpoint_name
        )
    except DataContextError:
        action_list = create_action_list(
            validation_settings_obj=validation_settings_obj
        )
        checkpoint = Checkpoint(
            name=validation_settings_obj.checkpoint_name,
            validation_definitions=[validation_definition],
            actions=action_list,
        )  # Note: a checkpoint combines validations with actions

        # Add checkpoint to data context for future use
        (
            validation_settings_obj.data_context.checkpoints.add(
                checkpoint=checkpoint
            )
        )
    return checkpoint


def create_and_configure_expectations(
    validation_rules_list: List[Rule],
    validation_settings_obj: ValidationSettings,
) -> None:
    # The suite should exist by now
    suite = validation_settings_obj.data_context.suites.get(
        name=validation_settings_obj.expectation_suite_name
    )

    for validation_rule in validation_rules_list:
        # Get the name of expectation as defined by GX
        gx_expectation_name = validation_rule["rule_name"]

        # Get the actual expectation as defined by GX
        gx_expectation = getattr(
            great_expectations.expectations.core,
            humps.pascalize(gx_expectation_name),
        )
        # Issue 50
        # TODO: drop pascalization, and require this as input check
        #  when ingesting JSON? Could be done via humps.is_pascalcase()

        for validation_parameter_dict in validation_rule["parameters"]:
            kwargs = {}
            # Issue 51
            # TODO/check: is this loop really necessary? Intuitively, I added
            #  the same expectation for each column - I didn't consider using
            #  the same expectation with different parameters
            for par_name, par_value in validation_parameter_dict.items():
                kwargs[par_name] = par_value
            suite.add_expectation(gx_expectation(**kwargs))


def validate(
    df: DataFrame,
    rules_dict: RulesDict,
    validation_settings_obj: ValidationSettings,
) -> CheckpointResult:
    """
    [explanation goes here]

    :param df: A list of DataFrame instances to process.
    :param rules_dict: a RulesDict object containing the
    data quality rules to be evaluated.
    :param validation_settings_obj: [explanation goes here]
    """
    # Make sure all attributes are aligned before validating
    validation_settings_obj.initialise_or_update_attributes()

    create_and_configure_expectations(
        validation_rules_list=rules_dict["rules"],
        validation_settings_obj=validation_settings_obj,
    )

    validation_definition = get_or_add_validation_definition(
        validation_settings_obj=validation_settings_obj,
    )
    print("***Starting validation definition run***")
    print(validation_definition.run(batch_parameters={"dataframe": df}))
    checkpoint = get_or_add_checkpoint(
        validation_settings_obj=validation_settings_obj,
        validation_definition=validation_definition,
    )

    batch_params = {"dataframe": df}
    return checkpoint.run(batch_parameters=batch_params)


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
    checkpoint_result = validate(
        df=df,
        rules_dict=rules_dict,
        validation_settings_obj=validation_settings_obj,
    )
    validation_output = checkpoint_result.describe_dict()

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
