from dataclasses import is_dataclass
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from src.dq_suite.common import (
    DataQualityRulesDict,
    DatasetDict,
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

    def test_initialisation_with_wrong_typed_rule_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert Rule(rule_name=123, parameters=[{}])

    def test_initialisation_with_wrong_typed_parameters_raises_type_error(self):
        with pytest.raises(TypeError):
            assert Rule(rule_name="the_rule", parameters=123)

    def test_rule_is_dataclass(self):
        assert is_dataclass(self.rule_obj)

    def test_get_value_from_rule_by_existing_key(self):
        assert self.rule_obj["rule_name"] == self.expected_rule_name
        assert self.rule_obj["parameters"] == self.expected_parameters

    def test_get_value_from_rule_by_non_existing_key_raises_key_error(self):
        with pytest.raises(KeyError):
            assert self.rule_obj["wrong_key"]


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

    def test_initialisation_with_wrong_typed_unique_identifier_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert RulesDict(
                unique_identifier=123,
                table_name=self.expected_table_name,
                rules_list=self.expected_rules_list,
            )

    def test_initialisation_with_wrong_typed_table_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert RulesDict(
                unique_identifier=self.expected_unique_identifier,
                table_name=123,
                rules_list=self.expected_rules_list,
            )

    def test_initialisation_with_wrong_typed_rules_list_raises_type_error(self):
        with pytest.raises(TypeError):
            assert RulesDict(
                unique_identifier=self.expected_unique_identifier,
                table_name=self.expected_table_name,
                rules_list=123,
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

    def test_get_value_from_rule_dict_by_non_existing_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            assert self.rules_dict_obj["wrong_key"]


class TestDatasetDict:
    expected_dataset_name = "the_dataset"
    expected_layer_name = "brons"
    dataset_obj = DatasetDict(name=expected_dataset_name, layer=expected_layer_name)

    def test_initialisation_with_wrong_typed_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DatasetDict(name=123, layer="brons")

    def test_initialisation_with_wrong_typed_layer_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DatasetDict(name="the_dataset", layer=123)

    def test_rule_is_dataclass(self):
        assert is_dataclass(self.dataset_obj)

    def test_get_value_from_rule_by_existing_key(self):
        assert self.dataset_obj["name"] == self.expected_dataset_name
        assert self.dataset_obj["layer"] == self.expected_layer_name

    def test_get_value_from_dataset_by_non_existing_key_raises_key_error(self):
        with pytest.raises(KeyError):
            assert self.dataset_obj["wrong_key"]


class TestDataQualityRulesDict:
    rule_obj = Rule(rule_name="the_rule", parameters=[{"q": 42}])
    expected_unique_identifier = "id"
    expected_table_name = "the_table"
    expected_rules_list = [rule_obj]
    rules_dict_obj = RulesDict(
        unique_identifier=expected_unique_identifier,
        table_name=expected_table_name,
        rules_list=expected_rules_list,
    )
    expected_rules_dict_obj_list = [rules_dict_obj]
    expected_dataset_name = "the_dataset"
    expected_layer_name = "brons"
    dataset_obj = DatasetDict(
        name=expected_dataset_name,
        layer=expected_layer_name
    )
    dataset_obj = {"name": "the_dataset", "layer": "brons"}
    data_quality_rules_dict = DataQualityRulesDict(
        dataset=dataset_obj,
        tables=expected_rules_dict_obj_list
    )

    def test_initialisation_with_wrong_typed_dataset_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DataQualityRulesDict(
                dataset=123,
                tables=[
                    RulesDict(
                        unique_identifier="id",
                        table_name="the_table",
                        rules_list=[
                            Rule(rule_name="the_rule", parameters=[{"q": 42}])
                        ],
                    )
                ]
            )

    def test_initialisation_with_wrong_typed_tables_raises_type_error(self):
        with pytest.raises(TypeError):
            assert DataQualityRulesDict(
                dataset={"name": "the_dataset", "layer": "brons"},
                tables=123
            )

    def test_get_value_from_data_quality_rules_dict_by_existing_key(self):
        assert (
            self.data_quality_rules_dict["tables"]
            == self.expected_rules_dict_obj_list
        )

    def test_get_value_from_rule_dict_by_non_existing_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            assert self.data_quality_rules_dict["wrong_key"]


def test_get_full_table_name():
    catalog_name = "catalog_dev"
    table_name = "the_table"
    expected_catalog_name = f"{catalog_name}.data_quality.{table_name}"

    name = get_full_table_name(catalog_name=catalog_name, table_name=table_name)
    assert name == expected_catalog_name
    with pytest.raises(ValueError):
        assert get_full_table_name(
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

    def test_initialisation_with_wrong_typed_spark_session_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=123,
                catalog_name="the_catalog",
                table_name="the_table",
                check_name="the_check",
            )

    def test_initialisation_with_wrong_typed_catalog_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name=123,
                table_name="the_table",
                check_name="the_check",
            )

    def test_initialisation_with_wrong_typed_table_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name=123,
                check_name="the_check",
            )

    def test_initialisation_with_wrong_typed_check_name_raises_type_error(self):
        with pytest.raises(TypeError):
            assert ValidationSettings(
                spark_session=self.spark_session_mock,
                catalog_name="the_catalog",
                table_name="the_table",
                check_name=123,
            )

    def test_set_expectation_suite_name(self):
        assert self.validation_settings_obj.expectation_suite_name is None

        self.validation_settings_obj._set_expectation_suite_name()
        assert (
            self.validation_settings_obj.expectation_suite_name
            == f"{self.validation_settings_obj.check_name}_expectation_suite"
        )

    def test_set_checkpoint_name(self):
        assert self.validation_settings_obj.checkpoint_name is None

        self.validation_settings_obj._set_checkpoint_name()
        assert (
            self.validation_settings_obj.checkpoint_name
            == f"{self.validation_settings_obj.check_name}_checkpoint"
        )

    def test_set_run_name(self):
        assert self.validation_settings_obj.run_name is None

        self.validation_settings_obj._set_run_name()
        assert (
            self.validation_settings_obj.run_name
            == f"%Y%m%d-%H%M%S-{self.validation_settings_obj.check_name}"
        )
