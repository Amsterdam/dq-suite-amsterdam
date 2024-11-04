from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from src.dq_suite import ValidationSettings
from src.dq_suite.validation import (
    create_action_list,
    create_and_configure_expectations,
    get_or_add_checkpoint,
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
    validation_settings_obj.initialise_or_update_name_parameters()
    return validation_settings_obj


class TestCreateActionList:
    def test_create_action_list(self):
        create_action_list


class TestGetOrAddCheckpoint:
    def test_get_or_add_checkpoint(self):
        get_or_add_checkpoint


class TestCreateAndConfigureExpectations:
    def test_create_and_configure_expectations(self):
        create_and_configure_expectations


class TestValidate:
    def test_validate(self):
        validate


class TestRun:
    def test_run(self):
        run
