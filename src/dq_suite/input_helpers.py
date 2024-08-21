import json
from typing import Any, Dict

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from .common import DataQualityRulesDict, Rule


def export_schema(dataset: str, spark: SparkSession) -> str:
    """
    Function exports a schema from Unity Catalog to be used by the Excel
    input form

    :param dataset: The name of the required dataset
    :param spark: The current SparkSession required for querying
    :return: schema_json: A JSON string with the schema of the required dataset
    """

    table_query = """
        SELECT table_name
        FROM system.information_schema.tables
        WHERE table_schema = {dataset}
    """
    tables = (
        spark.sql(table_query, dataset=dataset)
        .select("table_name")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    table_list = (
        "'" + "', '".join(tables) + "'"
    )  # creates a list of all tables, is used by the next query

    column_query = f"""
            SELECT column_name, table_name
            FROM system.information_schema.columns
            WHERE table_name IN ({table_list})
        """
    columns = spark.sql(column_query).select("column_name", "table_name")

    columns_list = []
    for table in tables:
        columns_table = (
            columns.filter(col("table_name") == table)
            .select("column_name")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        columns_dict = {"table_name": table, "attributes": columns_table}
        columns_list.append(columns_dict)

    output_dict = {"dataset": dataset, "tables": columns_list}

    return json.dumps(output_dict)


def fetch_schema_from_github(
    dq_rules_dict: DataQualityRulesDict,
) -> Dict[str, Any]:
    """
    Function fetches a schema from the GitHub Amsterdam schema using the
    dq_rules.

    :param dq_rules_dict: A dictionary with all DQ configuration.
    :return: schema_dict: A dictionary with the schema of the required tables.
    """

    schema_dict = {}
    for table in dq_rules_dict["tables"]:
        if "validate_table_schema_url" in table:
            url = table["validate_table_schema_url"]  # TODO: validate URL
            r = requests.get(url)
            schema = json.loads(r.text)
            schema_dict[table["table_name"]] = schema

    return schema_dict


def generate_dq_rules_from_schema(
    dq_rules_dict: DataQualityRulesDict,
) -> DataQualityRulesDict:
    """
    Function adds expect_column_values_to_be_of_type rule for each column of
    tables having schema_id and schema_url in dq_rules.

    :param dq_rules_dict: A dictionary with all DQ configuration.
    :return: A dictionary with all DQ configuration.
    """
    schema_dict = fetch_schema_from_github(dq_rules_dict=dq_rules_dict)

    for table in dq_rules_dict["tables"]:
        if "validate_table_schema" in table:
            schema_id = table["validate_table_schema"]
            table_name = table["table_name"]

            if table_name in schema_dict:
                schema = schema_dict[table_name]
                schema_columns = dict()

                if "schema" in schema and "properties" in schema["schema"]:
                    schema_columns = schema["schema"][
                        "properties"
                    ]  # separated tables - getting from table json
                elif (
                    "tables" in schema
                ):  # integrated tables - getting from dataset.json
                    for t in schema["tables"]:
                        if t["id"] == schema_id:
                            schema_columns = t["schema"]["properties"]
                            break

                if "schema" in schema_columns:
                    del schema_columns["schema"]

                for column, properties in schema_columns.items():
                    column_type = properties.get("type")
                    if column_type:
                        if column_type == "number":
                            rule_type = "IntegerType"
                        else:
                            rule_type = column_type.capitalize() + "Type"
                        rule = Rule(
                            rule_name="expect_column_values_to_be_of_type",
                            parameters=[{"column": column, "type_": rule_type}],
                        )
                        table["rules"].append(rule)

    return dq_rules_dict


def schema_to_json_string(dataset: str, spark: SparkSession) -> str:
    """
    Function exports a schema from Unity Catalog to be used by the Excel
    input form

    :param dataset: The name of the required dataset
    :param spark: The current SparkSession required for querying
    :return: schema_json: A JSON string with the schema of the required dataset
    """

    table_query = """
        SELECT table_name
        FROM system.information_schema.tables
        WHERE table_schema = {dataset}
    """
    tables = (
        spark.sql(table_query, dataset=dataset)
        .select("table_name")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    table_list = (
        "'" + "', '".join(tables) + "'"
    )  # creates a list of all tables, is used by the next query

    column_query = f"""
            SELECT column_name, table_name
            FROM system.information_schema.columns
            WHERE table_name IN ({table_list})
        """
    columns = spark.sql(column_query).select("column_name", "table_name")

    columns_list = []
    for table in tables:
        columns_table = (
            columns.filter(col("table_name") == table)
            .select("column_name")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        columns_dict = {"table_name": table, "attributes": columns_table}
        columns_list.append(columns_dict)

    output_dict = {"dataset": dataset, "tables": columns_list}

    return json.dumps(output_dict)


def read_data_quality_rules_from_json(file_path: str) -> str:
    with open(file_path, "r") as json_file:
        dq_rules_json_string = json_file.read()
    return dq_rules_json_string


def validate_and_load_data_quality_rules(
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


def data_quality_rules_json_string_to_dict(
    json_string: str,
) -> DataQualityRulesDict:
    """
    Function adds a mandatory line in case of a conditional rule

    :param json_string: A JSON string with all DQ configuration.
    :return: rule_json: A dictionary with all DQ configuration.
    """
    dq_rules_dict: DataQualityRulesDict = validate_and_load_data_quality_rules(
        dq_rules_json_string=json_string
    )

    for table in dq_rules_dict["tables"]:
        for rule in table["rules"]:
            for parameter in rule["parameters"]:
                if "row_condition" in parameter:
                    #  GX requires this statement for conditional rules when
                    #  using spark
                    parameter[
                        "condition_parser"
                    ] = "great_expectations__experimental__"

    return generate_dq_rules_from_schema(dq_rules_dict=dq_rules_dict)


def get_data_quality_rules_dict(file_path: str) -> DataQualityRulesDict:
    dq_rules_json_string = read_data_quality_rules_from_json(
        file_path=file_path
    )
    data_quality_rules_dict = validate_and_load_data_quality_rules(
        dq_rules_json_string=dq_rules_json_string
    )
    return data_quality_rules_dict
