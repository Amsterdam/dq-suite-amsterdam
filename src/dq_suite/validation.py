import datetime
from typing import List

from great_expectations import (
    Checkpoint,
    ExpectationSuite,
    ValidationDefinition,
    get_context,
)
from great_expectations.checkpoint import (
    MicrosoftTeamsNotificationAction,
    SlackNotificationAction,
)
from great_expectations.checkpoint.actions import CheckpointAction
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.datasource.fluent import SparkDatasource
from great_expectations.datasource.fluent.spark_datasource import DataFrameAsset
from great_expectations.exceptions import DataContextError
from great_expectations.expectations import core as gx_core
from pyspark.sql import DataFrame

from .common import Rule, RulesDict, ValidationSettings
from .input_helpers import (
    filter_validation_dict_by_table_name,
    get_data_quality_rules_dict,
    validate_data_quality_rules_dict,
)
from .output_transformations import (
    write_non_validation_tables,
    write_validation_table,
)


class ValidationRunner:
    def __init__(
        self,
        validation_settings_obj: ValidationSettings | None = None,
        data_context: AbstractDataContext | None = None,
        data_source: SparkDatasource | None = None,
        dataframe_asset: DataFrameAsset | None = None,
        validation_definition: ValidationDefinition | None = None,
        batch_definition: BatchDefinition | None = None,
        action_list: List | None = None,
    ):  # TODO: change all variables to private, once all logic has been moved
        #  inside this class

        if validation_settings_obj is None:
            raise ValueError(
                "No ValidationSettings instance has been " "provided."
            )
        if not isinstance(validation_settings_obj, ValidationSettings):
            raise ValueError(
                "No ValidationSettings instance has been " "provided."
            )

        # Copy ValidationSettings parameters
        self.spark_session = validation_settings_obj.spark_session
        self.catalog_name = validation_settings_obj.catalog_name
        self.table_name = validation_settings_obj.table_name
        self.check_name = validation_settings_obj.check_name
        self.data_context_root_dir = (
            validation_settings_obj.data_context_root_dir
        )
        self.data_source_name = validation_settings_obj.data_source_name
        self.expectation_suite_name = (
            validation_settings_obj.expectation_suite_name
        )
        self.checkpoint_name = validation_settings_obj.checkpoint_name
        self.run_name = validation_settings_obj.run_name
        self.validation_definition_name = (
            validation_settings_obj.validation_definition_name
        )
        self.batch_definition_name = (
            validation_settings_obj.batch_definition_name
        )
        self.send_slack_notification = (
            validation_settings_obj.send_slack_notification
        )
        self.slack_webhook = validation_settings_obj.slack_webhook
        self.send_ms_teams_notification = (
            validation_settings_obj.send_ms_teams_notification
        )
        self.ms_teams_webhook = validation_settings_obj.ms_teams_webhook
        self.notify_on = validation_settings_obj.notify_on

        # ValidationRunner-specific parameters
        self.data_context = data_context
        self.data_source = data_source
        self.dataframe_asset = dataframe_asset
        self.batch_definition = batch_definition
        self.validation_definition = validation_definition
        self.action_list = action_list

        self._set_data_context()

    def _set_data_context(self):  # pragma: no cover - uses part of GX
        self.data_context = get_context(
            project_config=DataContextConfig(
                store_backend_defaults=InMemoryStoreBackendDefaults(),
                analytics_enabled=False,
            )
        )

    def get_or_add_expectation_suite(self):  # pragma: no cover - uses part
        # of GX
        try:
            _ = self.data_context.suites.get(name=self.expectation_suite_name)
        except DataContextError:
            self.data_context.suites.add(
                suite=ExpectationSuite(name=self.expectation_suite_name)
            )

    def create_batch_definition(self):  # pragma: no cover - uses part of GX
        self.data_source = self.data_context.data_sources.add_or_update_spark(
            name=self.data_source_name
        )
        self.dataframe_asset = self.data_source.add_dataframe_asset(
            name=self.check_name
        )

        self.batch_definition = (
            self.dataframe_asset.add_batch_definition_whole_dataframe(
                name=self.batch_definition_name
            )
        )

    def create_validation_definition(
        self,
    ):  # pragma: no cover - uses part of GX
        try:
            validation_definition = (
                self.data_context.validation_definitions.get(
                    name=self.validation_definition_name
                )
            )
        except DataContextError:
            # Note: a validation definition combines data with a suite of
            # expectations
            validation_definition = ValidationDefinition(
                name=self.validation_definition_name,
                data=self.batch_definition,
                suite=self.data_context.suites.get(self.expectation_suite_name),
            )
            validation_definition = (
                self.data_context.validation_definitions.add(
                    validation=validation_definition
                )
            )

        self.validation_definition = validation_definition

    def _add_slack_notification_to_action_list(self):  # pragma: no cover - uses part of GX
        self.action_list.append(
            SlackNotificationAction(
                name="send_slack_notification",
                slack_webhook=self.slack_webhook,
                notify_on=self.notify_on,
                renderer={
                    "module_name": "great_expectations.render.renderer.slack_renderer",
                    "class_name": "SlackRenderer",
                },
            )
        )

    def _add_microsoft_teams_notification_to_action_list(self):  # pragma: no cover - uses part of GX
        self.action_list.append(
            MicrosoftTeamsNotificationAction(
                name="send_ms_teams_notification",
                microsoft_teams_webhook=self.ms_teams_webhook,
                notify_on=self.notify_on,
                renderer={
                    "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
                    "class_name": "MicrosoftTeamsRenderer",
                },
            )
        )

    def create_action_list(self):
        if self.send_slack_notification & (
                self.slack_webhook is not None
        ):
            self._add_slack_notification_to_action_list()

        if self.send_ms_teams_notification & (
                self.ms_teams_webhook is not None
        ):
            self._add_microsoft_teams_notification_to_action_list()


def get_or_add_checkpoint(
    validation_runner_obj: ValidationRunner,
) -> Checkpoint:
    try:
        checkpoint = validation_runner_obj.data_context.checkpoints.get(
            name=validation_runner_obj.checkpoint_name
        )
    except DataContextError:
        action_list = create_action_list(
            validation_runner_obj=validation_runner_obj
        )
        checkpoint = Checkpoint(
            name=validation_runner_obj.checkpoint_name,
            validation_definitions=[
                validation_runner_obj.validation_definition
            ],
            actions=action_list,
        )  # Note: a checkpoint combines validations with actions

        # Add checkpoint to data context for future use
        (
            validation_runner_obj.data_context.checkpoints.add(
                checkpoint=checkpoint
            )
        )
    return checkpoint


def create_and_configure_expectations(
    validation_rules_list: List[Rule],
    validation_runner_obj: ValidationRunner,
) -> None:
    suite = validation_runner_obj.data_context.suites.get(
        name=validation_runner_obj.expectation_suite_name
    )

    for validation_rule in validation_rules_list:
        # Get the name of expectation as defined by GX
        gx_expectation_name = validation_rule["rule_name"]
        gx_expectation_parameters: dict = validation_rule["parameters"]

        # Get the actual expectation as defined by GX
        gx_expectation = getattr(
            gx_core,
            gx_expectation_name,
        )
        suite.add_expectation(gx_expectation(**gx_expectation_parameters))


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
    validation_runner_obj = ValidationRunner(
        validation_settings_obj=validation_settings_obj
    )

    # Configure validation definition
    create_and_configure_expectations(
        validation_rules_list=rules_dict["rules"],
        validation_runner_obj=validation_runner_obj,
    )

    validation_runner_obj.create_batch_definition()
    validation_runner_obj.create_validation_definition()

    # Execute
    print("***Starting validation definition run***")
    checkpoint = get_or_add_checkpoint(
        validation_runner_obj=validation_runner_obj,
    )

    batch_params = {"dataframe": df}
    return checkpoint.run(batch_parameters=batch_params)


# TODO: modify so that validation_settings_obj is no longer an argument
def run(
    json_path: str, df: DataFrame, validation_settings_obj: ValidationSettings
) -> None:
    if not hasattr(df, "table_name"):
        df.table_name = validation_settings_obj.table_name

    # 1) extract the data quality rules to be applied...
    validation_dict = get_data_quality_rules_dict(file_path=json_path)
    validate_data_quality_rules_dict(data_quality_rules_dict=validation_dict)
    rules_dict = filter_validation_dict_by_table_name(
        validation_dict=validation_dict,
        table_name=validation_settings_obj.table_name,
    )
    if rules_dict is None:
        raise ValueError(
            f"No validations found for table_name "
            f"'{validation_settings_obj.table_name}' in JSON file at '"
            f"{json_path}'."
        )

    # 2) perform the validation on the dataframe
    checkpoint_result = validate(
        df=df,
        rules_dict=rules_dict,
        validation_settings_obj=validation_settings_obj,
    )
    validation_output = checkpoint_result.describe_dict()
    run_time = datetime.datetime.now()  # TODO: get from RunIdentifier object

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
        run_time=run_time,
    )
