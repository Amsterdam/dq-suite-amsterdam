from dataclasses import is_dataclass
from unittest.mock import Mock

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
)


def test_row_count_rule():
    assert row_count_rule.rule_name == "ExpectTableRowCountToBeBetween"
    assert "min_value" in row_count_rule.parameters
    assert "max_value" in row_count_rule.parameters


def test_column_match_rule():
    assert column_match_rule.rule_name == "ExpectTableColumnsToMatchSet"
    assert column_match_rule.parameters["exact_match"] is True
    assert "column_set" in column_match_rule.parameters


def test_column_type_rule():
    rule = column_type_rule("id", "Integer")
    assert rule.rule_name == "ExpectColumnValuesToBeOfType"
    assert rule.parameters == {"column": "id", "type_": "Integer"}


def test_datetime_regex_rule():
    rule = datetime_regex_rule("timestamp")
    assert rule.rule_name == "ExpectColumnValuesToMatchRegex"
    assert rule.parameters["column"] == "timestamp"
    assert "regex" in rule.parameters
    assert r"^\d{4}" in rule.parameters["regex"]  # basic check for datetime regex pattern


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
