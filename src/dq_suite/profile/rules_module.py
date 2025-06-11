from dq_suite.common import Rule
from typing import Dict, Any

row_count_rule = Rule(
    rule_name="ExpectTableRowCountToBeBetween",
    parameters={
        "min_value": "<TO BE FILLED IN AS INT>",  # 0 ?
        "max_value": "<TO BE FILLED IN AS INT>",  # take count of table rows directly?
    },
)

column_match_rule = Rule(
    rule_name="ExpectTableColumnsToMatchSet",
    parameters={
        "column_set": "[<COLUMNS TO BE FILLED IN>]",  # take all column names?
        "exact_match": True,
    },
)

column_compound_unique_rule = Rule(
    rule_name="ExpectCompoundColumnsToBeUnique",
    parameters={
        "column_list": ["<COLUMNS TO BE FILLED IN AS A LIST>"], 
    },
)


def column_type_rule(column: str, type_: str):
    return Rule(
        rule_name="ExpectColumnValuesToBeOfType",
        parameters={
            "column": column,
            "type_": type_,
        },
    )


def datetime_regex_rule(column_name: str) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToMatchRegex",
        parameters={
            "column": column_name,
            "regex": r"^(\d{4})(0[1-9]|1[0-2])(0[1-9]|[12]\d|30|31)",
        },
    )


def column_unique_rule(column_name: str) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToBeUnique",
        parameters={"column": column_name},
    )


def column_not_null_rule(column_name: str) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToNotBeNull",
        parameters={"column": column_name},
    )


def column_between_rule(column: str, min_val: Any, max_val: Any) -> Rule:
    return Rule(
        rule_name="ExpectColumnValuesToBeBetween",
        parameters={
            "column": column,
            "min_value": min_val,
            "max_value": max_val,
        },
    )


def column_greater_rule(column_name_A: str, column_name_B: str) -> Rule:
    return Rule(
        rule_name="ExpectColumnPairValuesAToBeGreaterThanB",
        parameters={
            "column_A": column_name_A,
            "column_B": column_name_B,
            "or_equal": False,
        },
    )


## "ExpectColumnValuesToBeInSet" ????
# def column_in_set_rule(column: str, allowed_values: list) -> Rule:
#     return Rule(
#         rule_name="ExpectColumnValuesToBeInSet",
#         parameters={
#             "column": column,
#             "value_set": allowed_values,
#         },
#     )