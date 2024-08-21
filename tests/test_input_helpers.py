import pytest

from src.dq_suite.input_helpers import read_data_quality_rules_from_json


class TestReadDataQualityRulesFromJson:
    dummy_file_path = "nonexistent_file_path"
    real_file_path = "tests/test_data/dq_rules.json"

    def test_read_data_quality_rules_from_json_raises_file_not_found_error(
        self,
    ):
        with pytest.raises(FileNotFoundError):
            read_data_quality_rules_from_json(file_path=self.dummy_file_path)

    @pytest.mark.skip()  # TODO: fix self.real_file_path
    def test_read_data_quality_rules_from_json_returns_json_string(self):
        dq_rules_json_string = read_data_quality_rules_from_json(
            file_path=self.real_file_path
        )
        assert type(dq_rules_json_string) is str
        assert "tables" in dq_rules_json_string
