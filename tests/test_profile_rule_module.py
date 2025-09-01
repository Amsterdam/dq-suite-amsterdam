import pytest
from pyspark.sql import SparkSession

from dq_suite.profile.rules_module import (
    row_count_rule,
    column_match_rule,
    column_unique_rule,
    column_not_null_rule,
    column_between_rule,
    column_type_rule,
    datetime_regex_rule,
    column_compound_unique_rule,
    column_values_in_set_rule,
)

def test_column_compound_unique_rule():
    assert column_compound_unique_rule.rule_name == "ExpectCompoundColumnsToBeUnique"
    assert "column_list" in column_compound_unique_rule.parameters


def test_row_count_rule():
    rule = row_count_rule(100)  # Call the function with a test value
    assert rule.rule_name == "ExpectTableRowCountToBeBetween"
    assert "min_value" in rule.parameters
    assert "max_value" in rule.parameters
    assert rule.parameters["max_value"] == 100


def test_column_match_rule():
    rule = column_match_rule(["id", "timestamp"])  # Call the function with a test list
    assert rule.rule_name == "ExpectTableColumnsToMatchSet"
    assert rule.parameters["exact_match"] is True
    assert "column_set" in rule.parameters
    assert rule.parameters["column_set"] == ["id", "timestamp"]


def test_column_type_rule():
    rule = column_type_rule("id", "Integer")
    assert rule.rule_name == "ExpectColumnValuesToBeOfType"
    assert rule.parameters == {"column": "id", "type_": "Integer"}


def test_datetime_regex_rule():
    rule = datetime_regex_rule("timestamp")
    assert rule.rule_name == "ExpectColumnValuesToMatchRegex"
    assert rule.parameters["column"] == "timestamp"
    assert "regex" in rule.parameters
    assert rule.parameters["regex"].startswith(r"^(\d{4})")

def test_column_unique_rule():
    rule = column_unique_rule("user_id")
    assert rule.rule_name == "ExpectColumnValuesToBeUnique"
    assert rule.parameters == {"column": "user_id"}


def test_column_not_null_rule():
    rule = column_not_null_rule("email")
    assert rule.rule_name == "ExpectColumnValuesToNotBeNull"
    assert rule.parameters == {"column": "email"}


def test_column_between_rule():
    rule = column_between_rule("age", 18, 99)
    assert rule.rule_name == "ExpectColumnValuesToBeBetween"
    assert rule.parameters == {
        "column": "age",
        "min_value": 18,
        "max_value": 99,
    }

def test_column_values_in_set_rule():
    rule = column_values_in_set_rule("isActief", ["Ja", "Nee"])
    assert rule.rule_name == "ExpectColumnValuesToBeInSet"
    assert "column" in rule.parameters
    assert "value_set" in rule.parameters 
    assert rule.parameters["value_set"] == ["Ja", "Nee"]