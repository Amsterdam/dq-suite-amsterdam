import json
from typing import Dict

from pyspark.sql import DataFrame

from dq_suite.common import DatasetDict, RulesDict
from dq_suite.profile.rules_module import (
    column_between_rule,
    column_compound_unique_rule,
    column_geometry_type_rule,
    column_match_rule,
    column_not_null_rule,
    column_type_rule,
    column_unique_rule,
    column_values_have_valid_geometry_rule,
    column_values_in_set_rule,
    column_values_not_empty_geometry_rule,
    datetime_regex_rule,
    row_count_rule,
)


def derive_team_from_dataset(dataset_name: str) -> dict:
    """
    Derive team information from the dataset name.

    Args:
        dataset_name (str): Name of the dataset (e.g., "dpmo_dev").

    Returns:
        dict: A dictionary containing:
            - teamid (str): Extracted team identifier (e.g., "dpmo")
            - teamname (str): Human-readable team name (e.g., "mo team")
            - teamdescription (str): Description of the team (same as teamname)
    """
    teamid = dataset_name.split("_")[0]
    teamname = teamid[-2:] + " team"

    return {
        "teamid": teamid,
        "teamname": teamname,
        "teamdescription": teamname,
    }


def normalize_numeric_type(col_type: str, details: dict) -> str:
    """
    Normalize a numeric column type based on profiling statistics.

    Args:
        col_type (str): Original column type from profiling (expected "Numeric").
        details (dict): Profiling details containing statistics such as "min" and "max".

    Returns:
        str:
            - "IntegerType" if both min and max are integer-like values
            - "DoubleType" otherwise
    """
    col_min = details.get("min")
    col_max = details.get("max")

    if isinstance(col_min, int) and isinstance(col_max, int):
        return "IntegerType"

    return "DoubleType"


def has_geometry_column(df: DataFrame, column_name: str) -> bool:
    """
    Check if the given DataFrame column contains at least one Geometry object.

    Args:
        df (DataFrame): Spark/Pandas DataFrame to check.
        column_name (str): Name of the column to inspect.

    Returns:
        bool: True if at least one value in the column is of type 'Geometry', else False.
    """
    return (
        df[column_name]
        .dropna()
        .apply(lambda x: type(x).__name__ == "Geometry")
        .any()
    )


def create_dq_rules(
    dataset_name: str,
    table_name: str,
    layer_name: str,
    profiling_json: Dict,
    df: DataFrame,
) -> RulesDict:
    """
    Create data quality rules based on the profiling report.
    """

    n = profiling_json["table"]["n"]
    columns = list(profiling_json["variables"].keys())
    rules = [
        column_compound_unique_rule,
        column_match_rule(columns),
        row_count_rule(n),
    ]

    for variable in profiling_json["variables"]:
        details = profiling_json["variables"][variable]
        col_type = details["type"]

        if "DateTime" in col_type:
            rules.append(datetime_regex_rule(variable))
            col_type = "TimestampType"

        if details.get("p_distinct", 0) == 1.0:
            rules.append(column_unique_rule(variable))

        if details.get("p_missing", 0) == 0.0:
            rules.append(column_not_null_rule(variable))

        if details.get("n_distinct", 0) < 10:
            value_counts = details.get("value_counts_without_nan", {})
            value_set = list(value_counts.keys())
            rules.append(column_values_in_set_rule(variable, value_set))

        if (
            "min" in details
            and "max" in details
            and "TimestampType" not in col_type
        ):
            rules.append(
                column_between_rule(variable, details["min"], details["max"])
            )

        if "Categorical" in col_type or "Text" in col_type:
            col_type = "StringType"
        if col_type == "Numeric":
            col_type = normalize_numeric_type(col_type, details)
        if has_geometry_column(df, variable):
            col_type = type(df[variable].dropna().iloc[0]).__name__
            geo_rules = [
                column_values_not_empty_geometry_rule(variable),
                column_geometry_type_rule(variable, col_type),
                column_values_have_valid_geometry_rule(variable),
            ]
            # Drop geo_query_template and description fields
            for r in geo_rules:
                r_dict = r.__dict__
                r_dict.pop("geo_query_template", None)
                r_dict.pop("description", None)

            rules.extend(geo_rules)
        rules.append(column_type_rule(variable, col_type))

    dq_rules = RulesDict(
        unique_identifier="<TO BE FILLED IN>",
        table_name=table_name,
        rules=rules,
    )

    team = derive_team_from_dataset(dataset_name)
    dataset = DatasetDict(name=dataset_name, layer=layer_name)

    dq_json = {
        "team": team,
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
