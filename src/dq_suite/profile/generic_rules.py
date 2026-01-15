import json
from typing import Dict
from pyspark.sql import DataFrame

from dq_suite.common import DatasetDict, RulesDict
from dq_suite.profile.rules_module import (
    column_between_rule,
    column_compound_unique_rule,
    column_match_rule,
    column_not_null_rule,
    column_type_rule,
    column_unique_rule,
    column_values_in_set_rule,
    datetime_regex_rule,
    row_count_rule,
    column_values_have_valid_geometry_rule,
    column_values_not_empty_geometry_rule,
    column_geometry_type_rule,
)


def has_geometry_column(df: DataFrame, column_name: str) -> bool:
    """
    Check if the given DataFrame column contains at least one Geometry object.

    Args:
        df (DataFrame): Spark/Pandas DataFrame to check.
        column_name (str): Name of the column to inspect.

    Returns:
        bool: True if at least one value in the column is of type 'Geometry', else False.
    """
    return df[column_name].dropna().apply(lambda x: type(x).__name__ == "Geometry").any()


def create_dq_rules(
    dataset_name: str, table_name: str, profiling_json: Dict, df : DataFrame
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
            col_min = details["min"]
            col_max = details["max"]
            if isinstance(col_min, int) and isinstance(col_max, int):
                col_type = "IntegerType"
            else:
                col_type = "DoubleType"       
        if has_geometry_column(df, variable):
            geo_rules = [
            column_values_not_empty_geometry_rule(variable),
            column_geometry_type_rule(variable, "GEOMETRY TYPE TO BE FILLED IN"),
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