import datetime
from typing import Dict, List, Literal

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
from pyspark.sql import DataFrame, SparkSession

from .common import Rule, RulesDict, ValidationSettings
from .output_transformations import (
    write_non_validation_tables,
    write_validation_table,
)
from .validation_input import (
    filter_validation_dict_by_table_name,
    get_data_quality_rules_dict,
    validate_data_quality_rules_dict,
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
        """
        spark_session: SparkSession object
        catalog_name: name of unity catalog
        table_name: name of table in unity catalog
        validation_name: name of data quality check
        data_context_root_dir: path to write GX data
        context - default "/dbfs/great_expectations/"
        data_context: a data context object
        expectation_suite_name: name of the GX expectation suite
        checkpoint_name: name of the GX checkpoint
        run_name: name of the data quality run
        slack_webhook: webhook, recommended to store in key vault
        ms_teams_webhook: webhook, recommended to store in key vault
        notify_on: when to send notifications, can be equal to "all",
        "success" or "failure"
        """

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
        self.validation_name = validation_settings_obj.validation_name
        self.data_context_root_dir = (
            validation_settings_obj.data_context_root_dir
        )
        self.data_source_name = validation_settings_obj._data_source_name
        self.expectation_suite_name = (
            validation_settings_obj._expectation_suite_name
        )
        self.checkpoint_name = validation_settings_obj._checkpoint_name
        self.run_name = validation_settings_obj._run_name
        self.validation_definition_name = (
            validation_settings_obj._validation_definition_name
        )
        self.batch_definition_name = (
            validation_settings_obj._batch_definition_name
        )
        self.slack_webhook = validation_settings_obj.slack_webhook
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

    def _set_data_context(self):
        self.data_context = get_context(
            project_config=DataContextConfig(
                store_backend_defaults=InMemoryStoreBackendDefaults(),
                analytics_enabled=False,
            )
        )

    def _get_or_add_expectation_suite(self) -> ExpectationSuite:
        try:  # If expectation_suite_name exists in data_context
            suite = self.data_context.suites.get(
                name=self.expectation_suite_name
            )
        except DataContextError:
            self.data_context.suites.add(
                suite=ExpectationSuite(name=self.expectation_suite_name)
            )
            suite = self.data_context.suites.get(
                name=self.expectation_suite_name
            )
        return suite

    @staticmethod
    def _get_gx_expectation_object(validation_rule: Rule):
        """
        From great_expectations.expectations.core, get the relevant class and
        instantiate an expectation object with the user-defined parameters
        """
        gx_expectation_name = validation_rule["rule_name"]
        gx_expectation_class = getattr(gx_core, gx_expectation_name)

        gx_expectation_parameters: dict = validation_rule["parameters"]
        return gx_expectation_class(**gx_expectation_parameters)

    def add_expectations_to_suite(self, validation_rules_list: List[Rule]):
        expectation_suite_obj = self._get_or_add_expectation_suite()  # Add if
        # it does not exist

        for validation_rule in validation_rules_list:
            gx_expectation_obj = self._get_gx_expectation_object(
                validation_rule=validation_rule
            )
            expectation_suite_obj.add_expectation(gx_expectation_obj)

    def create_batch_definition(self):  # pragma: no cover - only GX functions
        self.data_source = self.data_context.data_sources.add_or_update_spark(
            name=self.data_source_name
        )
        self.dataframe_asset = self.data_source.add_dataframe_asset(
            name=self.validation_name
        )

        self.batch_definition = (
            self.dataframe_asset.add_batch_definition_whole_dataframe(
                name=self.batch_definition_name
            )
        )

    def create_validation_definition(
        self,
    ):  # pragma: no cover - only GX functions
        """
        Note: a validation definition combines data with a suite of
        expectations. Therefore, this function can only be called if a
        batch definition and a (populated) expectation suite exist.
        """
        try:  # If validation_definition_name exists in data_context
            validation_definition = (
                self.data_context.validation_definitions.get(
                    name=self.validation_definition_name
                )
            )
        except DataContextError:
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

    def _add_slack_notification_to_action_list(
        self,
    ):
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

    def _add_microsoft_teams_notification_to_action_list(
        self,
    ):
        self.action_list.append(
            MicrosoftTeamsNotificationAction(
                name="send_ms_teams_notification",
                teams_webhook=self.ms_teams_webhook,
                notify_on=self.notify_on,
                renderer={
                    "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
                    "class_name": "MicrosoftTeamsRenderer",
                },
            )
        )

    def _create_action_list(self):
        self.action_list = list()

        if self.slack_webhook is not None:
            self._add_slack_notification_to_action_list()

        if self.ms_teams_webhook is not None:
            self._add_microsoft_teams_notification_to_action_list()

    def _get_or_add_checkpoint(
        self,
    ) -> Checkpoint:  # pragma: no cover - only GX functions
        try:
            checkpoint = self.data_context.checkpoints.get(
                name=self.checkpoint_name
            )  # If checkpoint_name exists in data_context
        except DataContextError:
            self._create_action_list()
            checkpoint = Checkpoint(
                name=self.checkpoint_name,
                validation_definitions=[self.validation_definition],
                actions=self.action_list,
            )  # Note: a checkpoint combines validations with actions

            # Add checkpoint to data context for future use
            (self.data_context.checkpoints.add(checkpoint=checkpoint))
        return checkpoint

    def run_validation(
        self, batch_parameters: Dict[str, DataFrame]
    ) -> CheckpointResult:  # pragma: no cover - only GX functions
        checkpoint = self._get_or_add_checkpoint()
        return checkpoint.run(batch_parameters=batch_parameters)


def validate(
    df: DataFrame,
    rules_dict: RulesDict,
    validation_settings_obj: ValidationSettings,
) -> CheckpointResult:  # pragma: no cover - only GX functions
    """
    Uses the rules_dict to populate an expectation suite, and applies these
    rules to a Spark Dataframe containing the data of interest. Returns the
    results of the validation.

    :param df: A list of DataFrame instances to process.
    :param rules_dict: a RulesDict object containing the
    data quality rules to be evaluated.
    :param validation_settings_obj: ValidationSettings object, contains all
    user input required for running a validation.
    """
    validation_runner_obj = ValidationRunner(
        validation_settings_obj=validation_settings_obj
    )

    validation_runner_obj.add_expectations_to_suite(
        validation_rules_list=rules_dict["rules"]
    )
    validation_runner_obj.create_batch_definition()
    validation_runner_obj.create_validation_definition()

    print("***Starting validation run***")
    return validation_runner_obj.run_validation(batch_parameters={"dataframe": df})


def run_validation(
    json_path: str,
    df: DataFrame,
    spark_session: SparkSession,
    catalog_name: str,
    table_name: str,
    validation_name: str,
    data_context_root_dir: str = "/dbfs/great_expectations/",
    slack_webhook: str | None = None,
    ms_teams_webhook: str | None = None,
    notify_on: Literal["all", "success", "failure"] = "failure",
) -> None:  # pragma: no cover - only GX functions
    """
    Main function for users of dq_suite.

    Runs a validation (specified by the rules in the JSON file located at [
    json_path]) on a dataframe [df], and writes the results to a data_quality
    table in [catalog_name].

    spark_session: SparkSession object
    catalog_name: name of unity catalog
    table_name: name of table in unity catalog
    validation_name: name of data quality check
    data_context_root_dir: path to write GX data
    context - default "/dbfs/great_expectations/"
    slack_webhook: webhook, recommended to store in key vault. If not None,
        a Slack notification will be sent
    ms_teams_webhook: webhook, recommended to store in key vault. If not None,
        an MS Teams notification will be sent
    notify_on: when to send notifications, can be equal to "all",
        "success" or "failure"
    """
    validation_settings_obj = ValidationSettings(
        spark_session=spark_session,
        catalog_name=catalog_name,
        table_name=table_name,
        validation_name=validation_name,
        data_context_root_dir=data_context_root_dir,
        slack_webhook=slack_webhook,
        ms_teams_webhook=ms_teams_webhook,
        notify_on=notify_on,
    )

    if not hasattr(df, "table_name"):
        # TODO/check: we can have df.table_name !=
        #  validation_settings_obj.table_name: is this wrong?
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

    # 2) ... perform the validation on the dataframe...
    checkpoint_result = validate(
        df=df,
        rules_dict=rules_dict,
        validation_settings_obj=validation_settings_obj,
    )
    validation_output = checkpoint_result.describe_dict()
    run_time = datetime.datetime.now()  # TODO: get from RunIdentifier object

    # 3) ... and write results to unity catalog
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
