from dataclasses import is_dataclass
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from src.dq_suite.common import (
    Rule,
    RulesDict,
    ValidationSettings,
    get_full_table_name,
)


class TestRule:
    expected_rule_name = "the_rule"
    expected_parameters = [{"q": 42}]
    rule_obj = Rule(
        rule_name=expected_rule_name, parameters=expected_parameters
    )

    def test_rule_is_dataclass(self):
        assert is_dataclass(self.rule_obj)

    def test_get_value_from_rule_by_existing_key(self):
        assert self.rule_obj["rule_name"] == self.expected_rule_name
        assert self.rule_obj["parameters"] == self.expected_parameters

    def test_get_value_from_rule_by_non_existing_key(self):
        with pytest.raises(KeyError):
            self.rule_obj["wrong_key"]


class TestRulesDict:
    rule_obj = Rule(rule_name="the_rule", parameters=[{"q": 42}])
    expected_unique_identifier = "id"
    expected_table_name = "the_table"
    expected_rules_list = [rule_obj]
    rules_dict_obj = RulesDict(
        unique_identifier=expected_unique_identifier,
        table_name=expected_table_name,
        rules_list=expected_rules_list,
    )

    def test_rules_dict_is_dataclass(self):
        assert is_dataclass(self.rules_dict_obj)

    def test_get_value_from_rule_dict_by_existing_key(self):
        assert (
            self.rules_dict_obj["unique_identifier"]
            == self.expected_unique_identifier
        )
        assert self.rules_dict_obj["table_name"] == self.expected_table_name
        assert self.rules_dict_obj["rules_list"] == self.expected_rules_list

    def test_get_value_from_rule_dict_by_non_existing_key(self):
        with pytest.raises(KeyError):
            self.rules_dict_obj["wrong_key"]


def test_get_full_table_name():
    catalog_name = "catalog_dev"
    table_name = "the_table"
    expected_catalog_name = f"{catalog_name}.data_quality.{table_name}"

    name = get_full_table_name(catalog_name=catalog_name, table_name=table_name)
    assert name == expected_catalog_name
    with pytest.raises(ValueError):
        get_full_table_name(
            catalog_name="catalog_wrong_suffix", table_name=table_name
        )


class TestValidationSettings:
    spark_session_mock = Mock(spec=SparkSession)
    validation_settings_obj = ValidationSettings(
        spark_session=spark_session_mock,
        catalog_name="the_catalog",
        table_name="the_table",
        check_name="the_check",
    )

    def test_initialise_or_update_attributes_updates_as_expected(self):
        expected_expectation_suite_name = (
            f"{self.validation_settings_obj.check_name}_expectation_suite"
        )
        expected_checkpoint_name = (
            f"{self.validation_settings_obj.check_name}_checkpoint"
        )
        expected_run_name = (
            f"%Y%m%d-%H%M%S-{self.validation_settings_obj.check_name}"
        )

        assert self.validation_settings_obj.expectation_suite_name is None
        assert self.validation_settings_obj.checkpoint_name is None
        assert self.validation_settings_obj.run_name is None

        self.validation_settings_obj.initialise_or_update_attributes()
        assert (
            self.validation_settings_obj.expectation_suite_name
            == expected_expectation_suite_name
        )
        assert (
            self.validation_settings_obj.checkpoint_name
            == expected_checkpoint_name
        )
        assert self.validation_settings_obj.run_name == expected_run_name

        assert (
            expected_expectation_suite_name
            in self.validation_settings_obj.data_context.list_expectation_suites()[
                0
            ]
            .__getstate__()
            .values()
        )
