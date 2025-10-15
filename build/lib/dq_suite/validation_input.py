import json
import os.path
from typing import Any

import humps
import validators

from .common import DataQualityRulesDict, RulesDict


def read_data_quality_rules_from_json(file_path: str) -> str:
    if not isinstance(file_path, str):
        raise TypeError(f"{file_path} should be of type 'str'")

    if not os.path.isfile(path=file_path):
        raise FileNotFoundError(
            f"'file_path' {file_path} does not point to a file"
        )

    with open(file_path, "r") as json_file:
        dq_rules_json_string = json_file.read()
    return dq_rules_json_string


def validate_data_quality_rules_dict(
    data_quality_rules_dict: Any | None,
) -> None:
    """
    Validates the format of the input JSON file containing all GX validations
    for a particular dataset.
    """
    # data_quality_rules_dict should (obviously) be a dict
    if not isinstance(data_quality_rules_dict, dict):
        raise TypeError("'data_quality_rules_dict' should be of type 'dict'")

    validate_dataset(data_quality_rules_dict=data_quality_rules_dict)
    validate_tables(data_quality_rules_dict=data_quality_rules_dict)

    for rules_dict in data_quality_rules_dict["tables"]:
        validate_rules_dict(rules_dict=rules_dict)

        if len(rules_dict["rules"]) == 0:
            validate_table_schema(rules_dict=rules_dict)
        else:
            for rule in rules_dict["rules"]:
                validate_rule(rule=rule)


def validate_dataset(data_quality_rules_dict: dict) -> None:
    if "dataset" not in data_quality_rules_dict:
        raise KeyError("No 'dataset' key found in data_quality_rules_dict")

    if not isinstance(data_quality_rules_dict["dataset"], dict):
        raise TypeError("'dataset' should be of type 'dict'")

    if "name" not in data_quality_rules_dict["dataset"]:
        raise KeyError(
            "No 'name' key found in data_quality_rules_dict['dataset']"
        )
    if "layer" not in data_quality_rules_dict["dataset"]:
        raise KeyError(
            "No 'layer' key found in data_quality_rules_dict['dataset']"
        )

    if not isinstance(data_quality_rules_dict["dataset"]["name"], str):
        raise TypeError("Dataset 'name' should be of type 'str'")
    if not isinstance(data_quality_rules_dict["dataset"]["layer"], str):
        raise TypeError("Dataset 'layer' should be of type 'str'")


def validate_tables(data_quality_rules_dict: Any) -> None:
    if "tables" not in data_quality_rules_dict:
        raise KeyError("No 'tables' key found in data_quality_rules_dict")

    if not isinstance(data_quality_rules_dict["tables"], list):
        raise TypeError("'tables' should be of type 'list'")


def validate_rules_dict(rules_dict: dict) -> None:
    # All RulesDict objects in 'tables' should...

    # ... be a dict
    if not isinstance(rules_dict, dict):
        raise TypeError(f"{rules_dict} should be of type 'dict'")

    # ... contain 'unique_identifier', 'table_name' and 'rules' keys
    if "unique_identifier" not in rules_dict:
        raise KeyError(f"No 'unique_identifier' key found in {rules_dict}")
    if "table_name" not in rules_dict:
        raise KeyError(f"No 'table_name' key found in {rules_dict}")
    if "rules" not in rules_dict:
        raise KeyError(f"No 'rules' key found in {rules_dict}")

    if not isinstance(rules_dict["rules"], list):
        raise TypeError(f"In {rules_dict}, 'rules' should be of type 'list'")


def validate_table_schema(rules_dict: dict) -> None:
    if "validate_table_schema" not in rules_dict:
        raise KeyError(f"No 'validate_table_schema' key found in {rules_dict}")
    if "validate_table_schema_url" not in rules_dict:
        raise KeyError(
            f"No 'validate_table_schema_url' key found in {rules_dict}"
        )
    if not validators.url(rules_dict["validate_table_schema_url"]):
        raise ValueError(
            f"The url specified in {rules_dict['validate_table_schema_url']} "
            f"is invalid"
        )


def validate_rule(rule: dict) -> None:
    # All Rule objects should...

    # ... be a dict
    if not isinstance(rule, dict):
        raise TypeError(f"{rule} should be of type 'dict'")

    # ... contain 'rule_name' and 'parameters' as keys
    if "rule_name" not in rule:
        raise KeyError(f"No 'rule_name' key found in {rule}")
    if "parameters" not in rule:
        raise KeyError(f"No 'parameters' key found in {rule}")

    # ... contain string-typed expectation names...
    if not isinstance(rule["rule_name"], str):
        raise TypeError(f"In {rule}, 'rule_name' should be of type 'str'")
    # ... as defined in GX (which switched to Pascal case in v1.0)
    if not humps.is_pascalcase(rule["rule_name"]):
        raise ValueError(
            f"The expectation name"
            f" '{rule['rule_name']}' "
            f"should "
            f"be written in Pascal case, "
            f"e.g. 'WrittenLikeThis' instead of "
            f"'written_like_this' "
            f"(hint: "
            f"'{humps.pascalize(rule['rule_name'])}')"
        )

    # 'parameters' should NOT be a list (as used in previous
    # versions), but a dict. The consequence of this is that the
    # same expectation should be repeated multiple times, with a
    # single dict of parameters each - decreasing the complexity
    # of the dataclass, but adding 'repeated' expectations
    if not isinstance(rule["parameters"], dict):
        raise TypeError(f"In {rule}, 'parameters' should be of type 'dict'")

    # severity check (if present, it must be valid)
    if "severity" in rule:
        allowed_severities = {"fatal", "error", "warning"}
        if rule["severity"] not in allowed_severities:
            raise ValueError(
                f"Invalid severity level '{rule['severity']}'. "
                f"Allowed values: {sorted(allowed_severities)}"
            )


def get_data_quality_rules_dict(file_path: str) -> DataQualityRulesDict:
    dq_rules_json_string = read_data_quality_rules_from_json(
        file_path=file_path
    )
    data_quality_rules_dict = json.loads(dq_rules_json_string)
    validate_data_quality_rules_dict(
        data_quality_rules_dict=data_quality_rules_dict
    )

    return data_quality_rules_dict


def filter_validation_dict_by_table_name(
    validation_dict: DataQualityRulesDict, table_name: str
) -> RulesDict | None:
    for rules_dict in validation_dict["tables"]:
        if rules_dict["table_name"] == table_name:
            # Only one RulesDict per table expected, so return the first match
            return rules_dict
    return None
