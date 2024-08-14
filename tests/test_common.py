from typing import Any

import pytest

from src.dq_suite.common import (
    Rule,
    RulesDict,
    RulesList,
    ValidationSettings,
    get_full_table_name,
)


class TestRule:
    rule_obj = Rule(rule_name="the_rule", parameters=[{"q": 42}])

    def test_get_value_from_rule_by_existing_key(self):
        rule_name = self.rule_obj["rule_name"]
        parameters = self.rule_obj["parameters"]

        assert rule_name == "the_rule"
        assert type(rule_name) is str

        assert parameters == [{"q": 42}]
        assert type(parameters) is list[dict[str, Any]]  # TODO: fix

    def test_get_value_from_rule_by_non_existing_key(self):
        with pytest.raises(KeyError):
            self.rule_obj["wrong_key"]


class TestRulesDict:
    rule_obj = Rule(rule_name="the_rule", parameters=[{"q": 42}])
    rules_dict_obj = RulesDict(
        unique_identifier="id", table_name="the_table", rules_list=[rule_obj]
    )

    def test_get_value_from_rule_dict_by_existing_key(self):
        unique_identifier = self.rules_dict_obj["unique_identifier"]
        table_name = self.rules_dict_obj["table_name"]
        rules_list = self.rules_dict_obj["rules_list"]

        assert unique_identifier == "id"
        assert type(unique_identifier) is str

        assert table_name == "the_table"
        assert type(table_name) is str

        assert rules_list == [self.rule_obj]
        assert type(rules_list) is RulesList  # TODO: fix
        assert type(rules_list[0]) is Rule

    def test_get_value_from_rule_dict_by_non_existing_key(self):
        with pytest.raises(KeyError):
            self.rules_dict_obj["wrong_key"]


def test_get_full_table_name():
    name = get_full_table_name(
        catalog_name="catalog_dev", table_name="the_table"
    )
    assert name == f"catalog_dev.data_quality.the_table"
    with pytest.raises(ValueError):
        get_full_table_name(
            catalog_name="catalog_wrong_suffix", table_name="the_table"
        )


class TestValidationSettings:
    validation_settings_obj = ValidationSettings(
        spark_session="test",  # TODO: fix local spark
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
