import pytest
from tests import TEST_DATA_FOLDER

from src.dq_suite.input_helpers import (
    load_data_quality_rules_from_json_string,
    read_data_quality_rules_from_json,
    validate_data_quality_rules_dict, validate_dataset, validate_tables,
    validate_rules_dict,
)


class TestReadDataQualityRulesFromJson:
    dummy_file_path = "nonexistent_file_path"
    real_file_path = f"{TEST_DATA_FOLDER}/dq_rules.json"

    def test_read_data_quality_rules_from_json_raises_file_not_found_error(
        self,
    ):
        with pytest.raises(FileNotFoundError):
            read_data_quality_rules_from_json(file_path=self.dummy_file_path)

    def test_read_data_quality_rules_from_json_returns_json_string(self):
        data_quality_rules_json_string = read_data_quality_rules_from_json(
            file_path=self.real_file_path
        )
        assert isinstance(data_quality_rules_json_string, str)


class TestLoadDataQualityRulesFromJsonString:
    real_file_path = f"{TEST_DATA_FOLDER}/dq_rules.json"
    data_quality_rules_json_string = read_data_quality_rules_from_json(
        file_path=real_file_path
    )

    # TODO: implement tests for all failure paths (and raise errors in
    #  read_data_quality_rules_from_json)

    def test_load_data_quality_rules_from_json_string(self):
        data_quality_rules_dict = load_data_quality_rules_from_json_string(
            dq_rules_json_string=self.data_quality_rules_json_string
        )
        assert isinstance(data_quality_rules_dict, dict)


class TestValidateDataQualityRulesDict:
    real_file_path = f"{TEST_DATA_FOLDER}/dq_rules.json"
    data_quality_rules_json_string = read_data_quality_rules_from_json(
        file_path=real_file_path
    )
    data_quality_rules_dict = load_data_quality_rules_from_json_string(
        dq_rules_json_string=data_quality_rules_json_string
    )

    def test_validate_data_quality_rules_dict_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_data_quality_rules_dict(
                data_quality_rules_dict="wrong_type")

    def test_validate_data_quality_rules_dict(self):
        validate_data_quality_rules_dict(
            data_quality_rules_dict=self.data_quality_rules_dict
        )


class TestValidateDataSet:
    real_file_path = f"{TEST_DATA_FOLDER}/dq_rules.json"
    data_quality_rules_json_string = read_data_quality_rules_from_json(
        file_path=real_file_path
    )
    data_quality_rules_dict = load_data_quality_rules_from_json_string(
        dq_rules_json_string=data_quality_rules_json_string
    )

    def test_validate_dataset_without_dataset_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_dataset(data_quality_rules_dict=
                             {"some_other_thing": "with_some_value"})

    def test_validate_dataset_without_dict_typed_value_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_dataset(data_quality_rules_dict=
                             {"dataset": "with_some_value"})

    def test_validate_dataset_without_name_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_dataset(data_quality_rules_dict=
                             {"dataset": dict()})

    def test_validate_dataset_without_layer_key_raises_key_error(
        self,
    ):
        with pytest.raises(KeyError):
            validate_dataset(data_quality_rules_dict=
                             {"dataset": {
                                 "name": 123
                             }
                              })

    def test_validate_dataset_without_str_typed_name_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_dataset(data_quality_rules_dict=
                             {"dataset": {
                                 "name": 123,
                                 "layer": 456
                             }
                              })

    def test_validate_dataset_without_str_typed_layer_raises_type_error(
        self,
    ):
        with pytest.raises(TypeError):
            validate_dataset(data_quality_rules_dict=
                             {"dataset": {
                                 "name": "the_dataset_name",
                                 "layer": 456
                             }
                              })

    def test_validate_dataset_works_as_expected(
        self,
    ):
        validate_dataset(data_quality_rules_dict=self.data_quality_rules_dict)


class TestValidateTables:
    real_file_path = f"{TEST_DATA_FOLDER}/dq_rules.json"
    data_quality_rules_json_string = read_data_quality_rules_from_json(
        file_path=real_file_path
    )
    data_quality_rules_dict = load_data_quality_rules_from_json_string(
        dq_rules_json_string=data_quality_rules_json_string
    )

    def test_validate_tables_without_tables_key_raises_key_error(
            self,
    ):
        with pytest.raises(KeyError):
            validate_tables(data_quality_rules_dict=
                            {"some_other_thing": "with_some_value"})

    def test_validate_tables_without_list_typed_value_raises_type_error(
            self,
    ):
        with pytest.raises(TypeError):
            validate_tables(data_quality_rules_dict=
                            {"tables": "with_some_value"})

    def test_validate_tables_works_as_expected(
        self,
    ):
        validate_tables(data_quality_rules_dict=self.data_quality_rules_dict)


class TestValidateRulesDict:
    real_file_path = f"{TEST_DATA_FOLDER}/dq_rules.json"
    data_quality_rules_json_string = read_data_quality_rules_from_json(
        file_path=real_file_path
    )
    data_quality_rules_dict = load_data_quality_rules_from_json_string(
        dq_rules_json_string=data_quality_rules_json_string
    )
    rules_dict = data_quality_rules_dict["tables"][0]

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
            validate_rules_dict(rules_dict={"unique_identifier": 123,
                                            "table_name": 456})

    def test_validate_rules_dict_without_list_typed_rules_raises_type_error(
            self,
    ):
        with pytest.raises(TypeError):
            validate_rules_dict(rules_dict={"unique_identifier": 123,
                                            "table_name": 456,
                                            "rules": 789})

    def test_validate_rules_dict_works_as_expected(
        self,
    ):
        validate_rules_dict(rules_dict=self.rules_dict)
