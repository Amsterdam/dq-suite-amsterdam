from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from src.dq_suite import ValidationSettings
from src.dq_suite.validation import (
    ValidationRunner,
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


@pytest.mark.usefixtures("validation_settings_obj")
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
        ) as mock_method:
            validation_runner_obj = ValidationRunner(
                validation_settings_obj=validation_settings_obj
            )
            mock_method.assert_called_once()

    def test_create_action_list(self):
        pass


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
