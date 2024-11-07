import pytest
from tests import TEST_DATA_FOLDER


@pytest.fixture
def rules_file_path():
    return f"{TEST_DATA_FOLDER}/dq_rules.json"


@pytest.fixture
def result_file_path():
    return f"{TEST_DATA_FOLDER}/dq_result.json"
