from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from src.dq_suite import ValidationSettings
from src.dq_suite.validation import (
    ValidationRunner,
    run,
    validate,
)


@pytest.fixture
def validation_settings_obj():
    spark_session_mock = Mock(spec=SparkSession)
    validation_settings_obj = ValidationSettings(
        spark_session=spark_session_mock,
        catalog_name="the_catalog",
        table_name="the_table",
        check_name="the_check",
    )
    return validation_settings_obj


@pytest.fixture
def validation_runner_obj(validation_settings_obj):
    with patch.object(
        target=ValidationRunner,
        attribute="_set_data_context",
    ) as mock_method:
        return ValidationRunner(validation_settings_obj=validation_settings_obj)


@pytest.mark.usefixtures("validation_settings_obj")
@pytest.mark.usefixtures("validation_runner_obj")
class TestValidationRunner:
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
        with patch.object(
            target=ValidationRunner,
            attribute="_set_data_context",
        ) as set_data_context_mock_method:
            validation_runner_obj = ValidationRunner(
                validation_settings_obj=validation_settings_obj
            )
            set_data_context_mock_method.assert_called_once()

    def test_create_action_list_with_slack_notification_with_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_slack_notification_to_action_list",
        ) as add_slack_action_mock_method:
            validation_runner_obj.send_slack_notification = True
            validation_runner_obj.slack_webhook = "the_slack_webhook"
            validation_runner_obj._create_action_list()
            add_slack_action_mock_method.assert_called_once()

    def test_create_action_list_without_slack_notification_with_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_slack_notification_to_action_list",
        ) as add_slack_action_mock_method:
            validation_runner_obj.send_slack_notification = False
            validation_runner_obj.slack_webhook = "the_slack_webhook"
            validation_runner_obj._create_action_list()
            add_slack_action_mock_method.assert_not_called()

    def test_create_action_list_with_slack_notification_without_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_slack_notification_to_action_list",
        ) as add_slack_action_mock_method:
            validation_runner_obj.send_slack_notification = True
            validation_runner_obj.slack_webhook = None
            validation_runner_obj._create_action_list()
            add_slack_action_mock_method.assert_not_called()

    def test_create_action_list_without_slack_notification_without_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_slack_notification_to_action_list",
        ) as add_slack_action_mock_method:
            validation_runner_obj.send_slack_notification = False
            validation_runner_obj.slack_webhook = None
            validation_runner_obj._create_action_list()
            add_slack_action_mock_method.assert_not_called()

    def test_create_action_list_with_ms_teams_notification_with_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_microsoft_teams_notification_to_action_list",
        ) as add_ms_teams_action_mock_method:
            validation_runner_obj.send_ms_teams_notification = True
            validation_runner_obj.ms_teams_webhook = "the_slack_webhook"
            validation_runner_obj._create_action_list()
            add_ms_teams_action_mock_method.assert_called_once()

    def test_create_action_list_without_ms_teams_notification_with_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_microsoft_teams_notification_to_action_list",
        ) as add_ms_teams_action_mock_method:
            validation_runner_obj.send_ms_teams_notification = False
            validation_runner_obj.ms_teams_webhook = "the_ms_teams_webhook"
            validation_runner_obj._create_action_list()
            add_ms_teams_action_mock_method.assert_not_called()

    def test_create_action_list_with_ms_teams_notification_without_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_microsoft_teams_notification_to_action_list",
        ) as add_ms_teams_action_mock_method:
            validation_runner_obj.send_ms_teams_notification = True
            validation_runner_obj.ms_teams_webhook = None
            validation_runner_obj._create_action_list()
            add_ms_teams_action_mock_method.assert_not_called()

    def test_create_action_list_without_ms_teams_notification_without_webhook(
        self, validation_runner_obj
    ):
        with patch.object(
            target=ValidationRunner,
            attribute="_add_microsoft_teams_notification_to_action_list",
        ) as add_ms_teams_action_mock_method:
            validation_runner_obj.send_ms_teams_notification = False
            validation_runner_obj.ms_teams_webhook = None
            validation_runner_obj._create_action_list()
            add_ms_teams_action_mock_method.assert_not_called()


class TestValidate:
    def test_validate(self):
        validate


class TestRun:
    def test_run(self):
        run
