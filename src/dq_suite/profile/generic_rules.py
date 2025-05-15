from typing import Dict, Any
import json
from dq_suite.common import RulesDict, Rule


def create_dq_rules(
    dataset_name: str, table_name: str, profiling_json: Dict
) -> RulesDict:
    """
    Create data quality rules based on the profiling report.
    """
    rules = [
        Rule(
            rule_name="ExpectTableRowCountToBeBetween",
            parameters={
                "min_value": "<TO BE FILLED IN AS INT>",
                "max_value": "<TO BE FILLED IN AS INT>",
            },
        ),
        Rule(
            rule_name="ExpectTableColumnsToMatchSet",
            parameters={
                "column_set": "[<COLUMNS TO BE FILLED IN>]",
                "exact_match": True,
            },
        ),
    ]

    for variable, details in profiling_json["variables"].items():

        if "DateTime" in details["type"]:
            rules.append(
                Rule(
                    rule_name="ExpectColumnValuesToMatchRegex",
                    parameters={
                        "column": variable,
                        "regex": "^(\\d{4})(0[1-9]|1[0-2])(0[1-9]|[12]\\d|30|31)",
                    },
                )
            )

        if "id" in variable:
            rules.append(
                Rule(
                    rule_name="ExpectColumnValuesToBeUnique",
                    parameters={"column": variable},
                )
            )

        p_missing = details.get("p_missing", 0)
        if p_missing >= 0.4:
            rules.append(
                Rule(
                    rule_name="ExpectColumnValuesToNotBeNull",
                    parameters={"column": variable},
                )
            )

    dq_rules = RulesDict(
        unique_identifier="<TO BE FILLED IN>",
        table_name=table_name,
        rules=rules,
    )

    return dq_rules


def save_rules_to_file(dq_rules: Dict, rule_path: str) -> None:
    """
    Save the data quality rules to a file.
    """
    with open(rule_path, "w") as f:
        json.dump(dq_rules, f, indent=4, default=lambda o: o.__dict__)