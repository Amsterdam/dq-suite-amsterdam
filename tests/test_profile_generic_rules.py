import pytest
from dq_suite.profile.generic_rules import create_dq_rules


@pytest.fixture
def profiling_input():
    return {
        "table": {"n": 1000},
        "variables": {
            "id": {
                "type": "Numeric",
                "p_distinct": 1.0,
                "p_missing": 0.0,
                "min": 1,
                "max": 100,
            },
            "timestamp": {
                "type": "DateTime",
                "p_missing": 0.0,
            },
            "status": {
                "type": "Categorical",
                "n_distinct": 3,
                "value_counts_without_nan": {
                    "open": 500,
                    "closed": 400,
                    "pending": 100,
                },
            },
            "amount": {
                "type": "Numeric",
                "min": 1,
                "max": 100,
            },
            "price": {
                "type": "Numeric",
                "min": 1.5,
                "max": 99.9,
            },
        },
    }


def test_dq_structure(profiling_input):
    dataset_name = "test_dataset"
    table_name = "test_table"
    dq_json = create_dq_rules(dataset_name, table_name, profiling_input)

    assert dq_json["dataset"].name == dataset_name
    assert dq_json["tables"][0].table_name == table_name


def test_missing_table_key_raises_error():
    """Ensure KeyError is raised when table metadata is missing."""
    profiling_input = {"variables": {"id": {"type": "Integer"}}}
    with pytest.raises(KeyError):
        create_dq_rules("dataset", "table", profiling_input)


def test_missing_variables_key_raises_error():
    """Ensure KeyError is raised when variables metadata is missing."""
    profiling_input = {"table": {"n": 1000}}
    with pytest.raises(KeyError):
        create_dq_rules("dataset", "table", profiling_input)


def test_rule_logic(profiling_input):
    dq_json = create_dq_rules("test_dataset", "test_table", profiling_input)
    rules = dq_json["tables"][0].rules
    rule_names = [rule.rule_name for rule in rules]
    columns = list(profiling_input["variables"].keys())

    # Check key expected rules
    assert "ExpectColumnValuesToBeUnique" in rule_names
    assert "ExpectColumnValuesToNotBeNull" in rule_names
    assert "ExpectColumnValuesToBeBetween" in rule_names
    assert "ExpectTableRowCountToBeBetween" in rule_names
    assert "ExpectColumnValuesToBeInSet" in rule_names
    assert any(
        rule.rule_name == "ExpectTableColumnsToMatchSet"
        and rule.parameters["column_set"] == columns
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["column"] == "id"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToMatchRegex"
        and rule.parameters["column"] == "timestamp"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["type_"] == "IntegerType"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["type_"] == "DoubleType"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["type_"] == "StringType"
        for rule in rules
    )