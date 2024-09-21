import pytest
from tests import TEST_DATA_FOLDER

from src.dq_suite.input_helpers import (
    load_data_quality_rules_from_json_string,
    read_data_quality_rules_from_json,
    validate_data_quality_rules_dict,
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
            validate_data_quality_rules_dict(data_quality_rules_dict="bla")

    def test_validate_data_quality_rules_dict(self):
        validate_data_quality_rules_dict(
            data_quality_rules_dict=self.data_quality_rules_dict
        )
