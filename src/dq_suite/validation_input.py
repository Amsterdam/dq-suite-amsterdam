import json
from typing import Any

import humps
import validators

from .common import DataQualityRulesDict


def read_data_quality_rules_from_json(file_path: str) -> str:
    with open(file_path, "r") as json_file:
        dq_rules_json_string = json_file.read()
    return dq_rules_json_string


def get_data_quality_rules_dict(file_path: str) -> DataQualityRulesDict:
    dq_rules_json_string = read_data_quality_rules_from_json(
        file_path=file_path
    )
    data_quality_rules_dict = load_data_quality_rules_from_json_string(
        dq_rules_json_string=dq_rules_json_string
    )
    validate_data_quality_rules_dict(
        data_quality_rules_dict=data_quality_rules_dict
    )

    return data_quality_rules_dict


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


def load_data_quality_rules_from_json_string(
    dq_rules_json_string: str,
) -> Any | None:
    """
    Deserializes a JSON document in string format, and prints one or more error
    messages in case a JSONDecodeError is raised.

    :param dq_rules_json_string: A JSON string with all DQ configuration.
    """
    try:
        return json.loads(dq_rules_json_string)

    except json.JSONDecodeError as e:
        error_message = str(e)
        print(f"Data quality check failed: {error_message}")
        if "Invalid control character at:" in error_message:
            print("Quota is missing in the JSON.")
        if "Expecting ',' delimiter:" in error_message:
            print(
                "Square brackets, Comma or curly brackets can be missing in "
                "the JSON."
            )
        if "Expecting ':' delimiter:" in error_message:
            print("Colon is missing in the JSON.")
        if "Expecting value:" in error_message:
            print("Rules' Value is missing in the JSON.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
