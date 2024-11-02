from src.dq_suite.validation import (
    create_action_list,
    create_and_configure_expectations,
    get_or_add_checkpoint,
    get_or_add_validation_definition,
    run,
    validate,
)


class TestGetOrAddValidationDefinition:
    def test_get_or_add_validation_definition(self):
        get_or_add_validation_definition


class TestCreateActionList:
    def test_create_action_list(self):
        create_action_list


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
