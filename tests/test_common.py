from src.dq_suite.common import ValidationSettings


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
