from typing import Dict, Any
import json
from dq_suite.common import RulesDict, Rule, DatasetDict

from dq_suite.profile.rules_module import (
    row_count_rule,
    column_match_rule,
    column_unique_rule,
    column_not_null_rule,
    column_between_rule,
    column_type_rule,
    regex_rule,
)


def create_dq_rules(
    dataset_name: str, table_name: str, profiling_json: Dict
) -> RulesDict:
    """
    Create data quality rules based on the profiling report.
    """
    rules = [row_count_rule, column_match_rule]

    for variable, details in profiling_json["variables"].items():
        col_type = details["type"]

        if "DateTime" in col_type:
            rules.append(regex_rule(variable))

        if details.get("p_distinct", 0) == 1.0:
            rules.append(column_unique_rule(variable))

        if details.get("p_missing", 0) == 0.0:
            rules.append(column_not_null_rule(variable))

        if "min" in details and "max" in details:
            rules.append(
                column_between_rule(variable, details["min"], details["max"])
            )

        rules.append(column_type_rule(variable, col_type))

    dq_rules = RulesDict(
        unique_identifier="<TO BE FILLED IN>",
        table_name=table_name,
        rules=rules,
    )

    dataset = DatasetDict(name=dataset_name, layer="<LAYER TO BE FILLED IN>")

    dq_json = {
        "dataset": dataset,
        "tables": [dq_rules],
    }

    return dq_json


def save_rules_to_file(dq_json: Dict, rule_path: str) -> None:
    """
    Save the data quality rules to a file.
    """
    with open(rule_path, "w") as f:
        json.dump(dq_json, f, indent=4, default=lambda o: o.__dict__)