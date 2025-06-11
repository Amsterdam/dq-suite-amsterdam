import pytest
from dq_suite.profile.generic_rules import create_dq_rules  
from pyspark.sql import SparkSession

@pytest.fixture
def profiling_input():
    return {
        "variables": {
            "id": {
                "type": "Integer",
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
                "type": "String",
            },
        }
    }


def test_dq_structure(profiling_input):
    dataset_name = "test_dataset"
    table_name = "test_table"
    dq_json = create_dq_rules(dataset_name, table_name, profiling_input)

    assert dq_json["dataset"].name == dataset_name
    assert dq_json["tables"][0].table_name == table_name


def test_rule_logic(profiling_input):
    dq_json = create_dq_rules("test_dataset", "test_table", profiling_input)
    rules = dq_json["tables"][0].rules

    rule_strs = [str(rule) for rule in rules]

    rule_names = [rule.rule_name for rule in rules]


    assert "ExpectColumnValuesToBeUnique" in rule_names
    assert "ExpectColumnValuesToNotBeNull" in rule_names
    assert "ExpectColumnValuesToBeBetween" in rule_names
    assert any(rule.rule_name == "ExpectColumnValuesToBeOfType" and rule.parameters["column"] == "id" for rule in rules)
    assert any(rule.rule_name == "ExpectColumnValuesToMatchRegex" and rule.parameters["column"] == "timestamp" for rule in rules)



