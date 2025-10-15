from dq_suite.common import Rule
from typing import Dict, Any


column_compound_unique_rule = Rule(
    rule_name="ExpectCompoundColumnsToBeUnique",
    severity="warning",
    parameters={
        "column_list": ["<COLUMNS TO BE FILLED IN AS A LIST>"],
    },
)


def column_match_rule(columns: list):
    return Rule(
        rule_name="ExpectTableColumnsToMatchSet",
        severity="warning",
        parameters={
            "column_set": columns,
            "exact_match": True,
        },
    )


def row_count_rule(n: int):
    return Rule(
        rule_name="ExpectTableRowCountToBeBetween",
        severity="warning",
        parameters={
            "min_value": 0,
            "max_value": n,
        },
    )


def column_type_rule(column: str, type_: str):
    return Rule(
        rule_name="ExpectColumnValuesToBeOfType",
        severity="warning",
        parameters={
            "column": column,
            "type_": type_,
        },
    )


def datetime_regex_rule(column_name: str) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToMatchRegex",
        severity="warning",
        parameters={
            "column": column_name,
            "regex": r"^(\d{4})(0[1-9]|1[0-2])(0[1-9]|[12]\d|30|31)",
        },
    )


def column_unique_rule(column_name: str) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToBeUnique",
        severity="warning",
        parameters={"column": column_name},
    )


def column_not_null_rule(column_name: str) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToNotBeNull",
        severity="warning",
        parameters={"column": column_name},
    )


def column_between_rule(column: str, min_val: Any, max_val: Any) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToBeBetween",
        severity="warning",
        parameters={
            "column": column,
            "min_value": min_val,
            "max_value": max_val,
        },
    )


def column_values_in_set_rule(column_name: str, value_set: list) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToBeInSet",
        severity="warning",
        parameters={
            "column": column_name,
            "value_set": value_set,
        },
    )