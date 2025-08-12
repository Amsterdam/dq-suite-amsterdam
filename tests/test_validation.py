import json
import os
from unittest.mock import Mock

import pytest
from chispa import assert_df_equality
from great_expectations.checkpoint import (
    MicrosoftTeamsNotificationAction,
    SlackNotificationAction,
)
from great_expectations.expectations import ExpectColumnDistinctValuesToEqualSet
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from src.dq_suite.common import Rule, ValidationSettings
from src.dq_suite.output_transformations import (
    create_metadata_dataframe,
    create_validation_result_dataframe,
)
from src.dq_suite.validation import ValidationRunner, validate


@pytest.fixture
def validation_settings_obj():
    spark_session_mock = Mock(spec=SparkSession)
    validation_settings_obj = ValidationSettings(
        spark_session=spark_session_mock,
        catalog_name="the_catalog",
        table_name="the_table",
        validation_name="the_validation",
        dataset_layer="the_layer",
        dataset_name="the_name",
        unique_identifier="the_id",
    )
    return validation_settings_obj


@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


@pytest.fixture
def validation_runner_obj(validation_settings_obj):
    return ValidationRunner(validation_settings_obj=validation_settings_obj)


@pytest.mark.usefixtures("spark")
@pytest.fixture
def simple_test_df(spark):
    # Create 13 rows: 4 with value 1, 4 with 2, 4 with 3, and 1 with 4
    data = [
        (i, val)
        for i, val in enumerate([1] * 4 + [2] * 4 + [3] * 4 + [4], start=1)
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


@pytest.fixture
def simple_rules_dict():
    filepath = os.path.join(
        os.path.dirname(__file__), "test_data", "dq_rules_simple.json"
    )
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def gx_checkpoint_result(
    validation_settings_obj, simple_test_df, simple_rules_dict
):
    rules_dict = simple_rules_dict["tables"][0]
    return validate(
        df=simple_test_df,
        rules_dict=rules_dict,
        validation_settings_obj=validation_settings_obj,
    )


@pytest.mark.usefixtures("validation_settings_obj")
@pytest.mark.usefixtures("validation_runner_obj")
class TestValidationRunner:
    """
    Note: the following methods are not yet tested, because they consist
    entirely of (a sequence of calls to) GX methods/functions:
    - create_batch_definition
    - create_validation_definition
    - _get_or_add_checkpoint
    - run
    """

    def test_initialisation_with_none_valued_validation_settings_raises_value_error(
        self,
    ):
        with pytest.raises(ValueError):
            assert ValidationRunner(validation_settings_obj=None)

    def test_initialisation_with_wrong_typed_validation_settings_raises_value_error(
        self,
    ):
        with pytest.raises(ValueError):
            assert ValidationRunner(validation_settings_obj=123)

    def test_initialisation_works_as_expected(self, validation_settings_obj):
        validation_runner_obj = ValidationRunner(
            validation_settings_obj=validation_settings_obj
        )

        # After setting all attribute values, a data_context should be created
        assert validation_runner_obj.data_context is not None

    def test_get_or_add_expectation_suite_works_as_expected(
        self, validation_runner_obj
    ):
        # Initially, no suites exist in the data context
        suites_list = list(validation_runner_obj.data_context.suites.all())
        assert len(suites_list) == 0

        # Create a new validation suite
        expected_expectation_suite_name = (
            "previously_undefined_validation_suite"
        )
        validation_runner_obj.expectation_suite_name = (
            expected_expectation_suite_name
        )
        first_suite = validation_runner_obj._get_or_add_expectation_suite()

        # Now, there should be 1 suite in the data context
        suites_list = list(validation_runner_obj.data_context.suites.all())
        assert len(suites_list) == 1
        assert suites_list[0] == first_suite

        # Calling the function again should return the existing validation suite
        second_suite = validation_runner_obj._get_or_add_expectation_suite()
        suites_list = list(validation_runner_obj.data_context.suites.all())
        assert len(suites_list) == 1
        assert suites_list[0] == first_suite
        assert first_suite == second_suite

    def test_get_nonexistent_gx_expectation_object_raises_attribute_error(
        self, validation_runner_obj
    ):
        with pytest.raises(AttributeError):
            the_rule = Rule(
                rule_name="NonExistentExpectation",
                severity="fatal",
                parameters={"column": "the_column", "value_set": [1, 2, 3]},
            )

            validation_runner_obj._get_gx_expectation_object(
                validation_rule=the_rule, table_name="the_table"
            )

    def test_get_gx_expectation_object(self, validation_runner_obj):
        the_rule = Rule(
            rule_name="ExpectColumnDistinctValuesToEqualSet",
            severity="fatal",
            parameters={"column": "the_column", "value_set": [1, 2, 3]},
        )
        the_expectation_object = (
            validation_runner_obj._get_gx_expectation_object(
                validation_rule=the_rule, table_name="the_table"
            )
        )

        assert isinstance(
            the_expectation_object, ExpectColumnDistinctValuesToEqualSet
        )
        assert the_expectation_object.column == the_rule["parameters"]["column"]
        assert (
            the_expectation_object.value_set
            == the_rule["parameters"]["value_set"]
        )

    def test_add_expectations_to_suite_works_as_expected(
        self, validation_runner_obj
    ):
        validation_rules_list = [
            Rule(
                rule_name="ExpectColumnDistinctValuesToEqualSet",
                severity="fatal",
                parameters={"column": "the_column", "value_set": [1, 2, 3]},
            )
        ]
        validation_runner_obj.add_expectations_to_suite(
            validation_rules_list=validation_rules_list
        )
        suites_list = list(validation_runner_obj.data_context.suites.all())
        expectations_list = suites_list[0]["expectations"]
        assert len(expectations_list) == 1
        assert isinstance(
            expectations_list[0], ExpectColumnDistinctValuesToEqualSet
        )

    def test_create_batch_definition(self, validation_runner_obj):
        # Initially, no batch definitions exist in the data context

        # validation_runner_obj.create_batch_definition()

        # TODO: mock use of spark for
        #  data_context.data_sources.add_or_update_spark
        pass

    def test_create_validation_definition(self, validation_runner_obj):
        # TODO: mock use of batch_definition for ValidationDefinition
        pass

    def test_create_action_list_with_slack_webhook(self, validation_runner_obj):
        # Initially, there are no actions in the action_list parameter
        assert validation_runner_obj.action_list is None

        validation_runner_obj.slack_webhook = "the_slack_webhook"
        validation_runner_obj._create_action_list()

        # Now there should be 1 SlackNotificationAction in the action_list
        assert isinstance(validation_runner_obj.action_list, list)
        assert len(validation_runner_obj.action_list) == 1
        assert isinstance(
            validation_runner_obj.action_list[0], SlackNotificationAction
        )

    def test_create_action_list_without_slack_webhook(
        self, validation_runner_obj
    ):
        # Initially, there are no actions in the action_list parameter
        assert validation_runner_obj.action_list is None

        validation_runner_obj.slack_webhook = None
        validation_runner_obj._create_action_list()

        # Now there should still be no actions in the action_list parameter
        assert isinstance(validation_runner_obj.action_list, list)
        assert len(validation_runner_obj.action_list) == 0

    def test_create_action_list_with_ms_teams_webhook(
        self, validation_runner_obj
    ):
        # Initially, there are no actions in the action_list parameter
        assert validation_runner_obj.action_list is None

        validation_runner_obj.ms_teams_webhook = "the_ms_teams_webhook"
        validation_runner_obj._create_action_list()

        # Now there should be 1 MicrosoftTeamsNotificationAction in the action_list
        assert isinstance(validation_runner_obj.action_list, list)
        assert len(validation_runner_obj.action_list) == 1
        assert isinstance(
            validation_runner_obj.action_list[0],
            MicrosoftTeamsNotificationAction,
        )

    def test_create_action_list_without_ms_teams_webhook(
        self, validation_runner_obj
    ):
        # Initially, there are no actions in the action_list parameter
        assert validation_runner_obj.action_list is None

        validation_runner_obj.ms_teams_webhook = None
        validation_runner_obj._create_action_list()

        # Now there should still be no actions in the action_list parameter
        assert isinstance(validation_runner_obj.action_list, list)
        assert len(validation_runner_obj.action_list) == 0

    def test_get_or_add_checkpoint(self, validation_runner_obj):
        # TODO: mock use of ValidationDefinition for Checkpoint
        pass


@pytest.mark.usefixtures("gx_checkpoint_result")
@pytest.mark.usefixtures("simple_rules_dict")
@pytest.mark.usefixtures("spark")
@pytest.mark.usefixtures("validation_settings_obj")
class TestOutputModelValidity:
    """
    Note: this is an integration test that checks for alignment between the DQ suite and Great Expectations without invoking Unity Catalog.
    """

    def test_create_matching_regelids(
        self,
        simple_rules_dict,
        spark,
        gx_checkpoint_result,
        validation_settings_obj,
    ):
        # Create regel ID from rule JSON
        regel_df = create_metadata_dataframe(
            metadata_table_name="regel",
            dq_rules_dict=simple_rules_dict,
            spark_session=spark,
        )

        # Create regel ID from GX validation results
        validation_settings_obj.spark_session = spark
        validation_df = create_validation_result_dataframe(
            df=[],
            checkpoint_result=gx_checkpoint_result,
            validation_table_name="validatie",
            validation_settings_obj=validation_settings_obj,
        )

        assert_df_equality(
            regel_df.select("regelId"), validation_df.select("regelId")
        )
