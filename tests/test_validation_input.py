import json
from unittest.mock import patch

import pytest
from tests import TEST_DATA_FOLDER

from src.dq_suite import validation_input
from src.dq_suite.validation_input import (
    filter_validation_dict_by_table_name,
    get_data_quality_rules_dict,
    read_data_quality_rules_from_json,
    validate_data_quality_rules_dict,
    validate_dataset,
    validate_rule,
    validate_rules_dict,
    validate_table_schema,
    validate_tables,
)


@pytest.fixture
def real_json_file_path():
    return f"{TEST_DATA_FOLDER}/dq_rules.json"


@pytest.fixture
def real_non_json_file_path():
    return f"{TEST_DATA_FOLDER}/test_schema.py"


@pytest.fixture
def real_json_file_invalid_formatting_path():
    return f"{TEST_DATA_FOLDER}/dq_rules_invalid_formatting.json"


@pytest.fixture
def data_quality_rules_json_string(real_json_file_path):
    return read_data_quality_rules_from_json(file_path=real_json_file_path)


@pytest.fixture
def data_quality_rules_dict(data_quality_rules_json_string):
    return json.loads(data_quality_rules_json_string)


@pytest.fixture
def validated_data_quality_rules_dict(data_quality_rules_dict):
    """
    Note: the normal flow validates the data quality rules dict before
    using it - hence this fixture.
    """
    validate_data_quality_rules_dict(
        data_quality_rules_dict=data_quality_rules_dict
    )
    return data_quality_rules_dict


@pytest.fixture
def rules_dict(data_quality_rules_dict):
    return data_quality_rules_dict["tables"][0]


@pytest.mark.usefixtures("real_json_file_path")
@pytest.mark.usefixtures("real_non_json_file_path")
class TestReadDataQualityRulesFromJson:
    def test_read_data_quality_rules_from_json_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            read_data_quality_rules_from_json(file_path=123)

    def test_read_data_quality_rules_from_json_raises_file_not_found_error(
        self,
    ):
        with pytest.raises(FileNotFoundError):
            read_data_quality_rules_from_json(file_path="nonexistent_file_path")

    def test_read_data_quality_rules_from_json_raises_value_error_for_non_json_file(
        self, real_non_json_file_path
    ):
        non_json_string = read_data_quality_rules_from_json(
            file_path=real_non_json_file_path
        )
        assert isinstance(non_json_string, str)  # It's a string...
        with pytest.raises(ValueError):  # ... but not valid JSON
            json.loads(non_json_string)

    def test_read_data_quality_rules_from_json_returns_json_string(
        self, real_json_file_path
    ):
        data_quality_rules_json_string = read_data_quality_rules_from_json(
            file_path=real_json_file_path
        )
        assert isinstance(data_quality_rules_json_string, str)


@pytest.mark.usefixtures("data_quality_rules_dict")
class TestValidateDataQualityRulesDict:
    def test_validate_data_quality_rules_dict_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_data_quality_rules_dict(
                data_quality_rules_dict="wrong_type"
            )

    def test_rules_dict_without_rules_field_results_in_table_schema_validation(
        self, data_quality_rules_dict
    ):
        with patch.object(
            target=validation_input,
            attribute="validate_table_schema",
        ) as validate_table_schema_mock:
            validate_data_quality_rules_dict(
                data_quality_rules_dict=data_quality_rules_dict
            )
            validate_table_schema_mock.assert_called_once()

    def test_validate_data_quality_rules_dict(self, data_quality_rules_dict):
        validate_data_quality_rules_dict(
            data_quality_rules_dict=data_quality_rules_dict
        )


@pytest.mark.usefixtures("data_quality_rules_dict")
class TestValidateDataSet:
    def test_validate_dataset_without_dataset_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_dataset(
                data_quality_rules_dict={"some_other_thing": "with_some_value"}
            )

    def test_validate_dataset_without_dict_typed_value_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_dataset(
                data_quality_rules_dict={"dataset": "with_some_value"}
            )

    def test_validate_dataset_without_name_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_dataset(data_quality_rules_dict={"dataset": dict()})

    def test_validate_dataset_without_layer_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_dataset(data_quality_rules_dict={"dataset": {"name": 123}})

    def test_validate_dataset_without_str_typed_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_dataset(
                data_quality_rules_dict={"dataset": {"name": 123, "layer": 456}}
            )

    def test_validate_dataset_without_str_typed_layer_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_dataset(
                data_quality_rules_dict={
                    "dataset": {"name": "the_dataset_name", "layer": 456}
                }
            )

    def test_validate_dataset_works_as_expected(self, data_quality_rules_dict):
        validate_dataset(data_quality_rules_dict=data_quality_rules_dict)


@pytest.mark.usefixtures("data_quality_rules_dict")
class TestValidateTables:
    def test_validate_tables_without_tables_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_tables(
                data_quality_rules_dict={"some_other_thing": "with_some_value"}
            )

    def test_validate_tables_without_list_typed_value_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_tables(
                data_quality_rules_dict={"tables": "with_some_value"}
            )

    def test_validate_tables_works_as_expected(self, data_quality_rules_dict):
        validate_tables(data_quality_rules_dict=data_quality_rules_dict)


@pytest.mark.usefixtures("rules_dict")
class TestValidateRulesDict:
    def test_validate_rules_dict_without_dict_typed_value_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_rules_dict(rules_dict="not_a_dict")

    def test_validate_rules_dict_without_unique_identifier_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_rules_dict(rules_dict=dict())

    def test_validate_rules_dict_without_table_name_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_rules_dict(rules_dict={"unique_identifier": 123})

    def test_validate_rules_dict_without_rules_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_rules_dict(
                rules_dict={"unique_identifier": 123, "table_name": 456}
            )

    def test_validate_rules_dict_without_list_typed_rules_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_rules_dict(
                rules_dict={
                    "unique_identifier": 123,
                    "table_name": 456,
                    "rules": 789,
                }
            )

    def test_validate_rules_dict_works_as_expected(self, rules_dict):
        validate_rules_dict(rules_dict=rules_dict)


class TestValidateTableSchema:
    def test_validate_table_schema_without_validate_table_schema_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_table_schema(rules_dict=dict())

    def test_validate_table_schema_without_validate_table_schema_url_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_table_schema(
                rules_dict={"validate_table_schema": "some_table_name"}
            )

    def test_validate_table_schema_with_invalid_url_raises_value_error(self):
        with pytest.raises(ValueError):
            validate_table_schema(
                rules_dict={
                    "validate_table_schema": "some_table_name",
                    "validate_table_schema_url": "some_invalid_url",
                }
            )

    def test_validate_table_schema_works_as_expected(
        self,
    ):
        validate_table_schema(
            rules_dict={
                "validate_table_schema": "some_table_name",
                "validate_table_schema_url": "https://www.someurl.nl",
            }
        )


class TestValidateRule:
    def test_validate_rule_without_dict_typed_rule_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_rule(rule="not_a_dict")

    def test_validate_rule_without_rule_name_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_rule(rule=dict())

    def test_validate_rule_without_parameters_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_rule(rule={"rule_name": 123})

    def test_validate_rule_without_string_typed_rule_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_rule(rule={"rule_name": 123, "parameters": 456})

    def test_validate_rule_without_pascal_case_rule_name_raises_value_error(
        self,
    ):
        with pytest.raises(ValueError):
            validate_rule(rule={"rule_name": "the_rule", "parameters": 456})

    def test_validate_rule_without_dict_typed_parameters_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_rule(rule={"rule_name": "TheRule", "parameters": 456})

    def test_validate_rule_works_as_expected(
        self,
    ):
        validate_rule(
            rule={
                "rule_name": "TheRule",
                "parameters": {"some_key": "some_value"},
            }
        )
    def test_validate_rule_with_invalid_severity_raises_value_error(
        self,
    ):
        with pytest.raises(ValueError, match="Invalid severity level"):
            validate_rule(
                rule={
                    "rule_name": "TheRule",
                    "parameters": {"some_key": "some_value"},
                    "severity": "critical"
                }
            )

    def test_validate_rule_works_as_expected(
        self,
    ):
        validate_rule(
            rule={
                "rule_name": "TheRule",
                "parameters": {"some_key": "some_value"},
                "severity": "fatal", 
            }
        )


@pytest.mark.usefixtures("real_json_file_invalid_formatting_path")
@pytest.mark.usefixtures("real_json_file_path")
class TestGetDataQualityRulesDict:
    def test_get_data_quality_rules_dict_raises_json_decode_error(
        self, real_json_file_invalid_formatting_path
    ):
        with pytest.raises(json.JSONDecodeError):
            get_data_quality_rules_dict(
                file_path=real_json_file_invalid_formatting_path
            )

    def test_get_data_quality_rules_dict_returns_dict(
        self, real_json_file_path
    ):
        data_quality_rules_dict = get_data_quality_rules_dict(
            file_path=real_json_file_path
        )
        assert isinstance(data_quality_rules_dict, dict)


@pytest.mark.usefixtures("validated_data_quality_rules_dict")
class TestFilterValidationDictByTableName:
    def test_filter_validation_dict_by_table_name_returns_none_for_nonexistent_table(
        self, validated_data_quality_rules_dict
    ):
        the_nonexistent_table_dict = filter_validation_dict_by_table_name(
            validation_dict=validated_data_quality_rules_dict,
            table_name="the_nonexistent_table",
        )
        assert the_nonexistent_table_dict is None

    def test_filter_validation_dict_by_table_name(
        self, validated_data_quality_rules_dict
    ):
        the_table_dict = filter_validation_dict_by_table_name(
            validation_dict=validated_data_quality_rules_dict,
            table_name="the_table",
        )
        assert the_table_dict is not None
        assert "unique_identifier" in the_table_dict
        assert "table_name" in the_table_dict
        assert "rules" in the_table_dict
