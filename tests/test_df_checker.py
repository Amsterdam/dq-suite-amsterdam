import pytest

from src.dq_suite.df_checker import read_data_quality_rules_from_json, ValidationSettings


class TestValidationSettings:
    validation_settings_obj = ValidationSettings(
        spark_session="test",  # TODO: fix local spark
        catalog_name="the_catalog",
        table_name="the_table",
        check_name="the_check",
    )

    def test_initialise_or_update_attributes_updates_as_expected(self):
        expected_suite_name = "the_check_expectation_suite"

        assert self.validation_settings_obj.expectation_suite_name is None
        assert self.validation_settings_obj.checkpoint_name is None
        assert self.validation_settings_obj.run_name is None
        self.validation_settings_obj.initialise_or_update_attributes()
        assert (self.validation_settings_obj.expectation_suite_name ==
                expected_suite_name)
        assert (expected_suite_name in
                self.validation_settings_obj.data_context
                .list_expectation_suites()[0].__getstate__().values())


class TestReadDataQualityRulesFromJson:
    dummy_file_path = "nonexistent_file_path"
    real_file_path = "test_data/dq_rules.json"

    def test_read_data_quality_rules_from_json_raises_file_not_found_error(
            self):
        with pytest.raises(FileNotFoundError):
            read_data_quality_rules_from_json(file_path=self.dummy_file_path)

    def test_read_data_quality_rules_from_json_returns_json_string(
            self):
        dq_rules_json_string = read_data_quality_rules_from_json(
            file_path=self.real_file_path)
        assert type(dq_rules_json_string) is str
        assert "tables" in dq_rules_json_string
