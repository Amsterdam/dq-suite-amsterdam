"""
The contents of this other.py script used to be in validation_input.py.
They were moved here, because none of them are used by an actual validation
run - instead, they are more like general helper functions for ad-hoc tasks.
"""


import json
from typing import Any, Dict, List

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from .common import DataQualityRulesDict, Rule


def get_table_name_list_from_unity_catalog(
    dataset: str, spark: SparkSession
) -> List[str]:
    """
    Returns a list of all table names present in a schema (e.g. 'bronze') in
    Unity Catalog.
    """
    table_query = """
            SELECT table_name
            FROM system.information_schema.tables
            WHERE table_schema = {dataset}
        """
    return (
        spark.sql(sqlQuery=table_query, dataset=dataset)
        .select("table_name")
        .rdd.flatMap(lambda x: x)
        .collect()
    )


def create_dataframe_containing_all_column_names_in_tables(
    table_name_list: List[str], spark: SparkSession
) -> DataFrame:
    """
    Returns a dataframe containing all column names in all tables.
    Query once, and (in a next step) loop multiple times over the dataframe.
    """

    table_name_sql_string = "'" + "', '".join(table_name_list) + "'"

    column_query = f"""
                SELECT column_name, table_name
                FROM system.information_schema.columns
                WHERE table_name IN ({table_name_sql_string})
            """
    return spark.sql(sqlQuery=column_query).select("column_name", "table_name")


def get_column_name_list(
    df_columns_tables: DataFrame, table_name: str
) -> List[str]:
    """
    Returns a list of all column names in a particular table.
    """
    return (
        df_columns_tables.filter(col("table_name") == lit(table_name))
        .select("column_name")
        .rdd.flatMap(lambda x: x)
        .collect()
    )


def get_all_table_name_to_column_names_mappings(
    table_name_list: List[str], df_columns_tables: DataFrame
) -> List[Dict[str, str | List[str]]]:
    """
    Returns a list of dictionaries. Each dictionary contains information
    about 1 table: the table name, and the column names within that table.
    """
    list_of_all_table_name_to_column_names_mappings = []
    for table_name in table_name_list:
        column_name_list = get_column_name_list(
            df_columns_tables=df_columns_tables, table_name=table_name
        )
        table_name_to_column_names_mapping: Dict[str, str | List[str]] = {
            "table_name": table_name,
            "attributes": column_name_list,
        }
        list_of_all_table_name_to_column_names_mappings.append(
            table_name_to_column_names_mapping
        )
    return list_of_all_table_name_to_column_names_mappings


def export_schema_to_json_string(
    dataset: str, spark: SparkSession, *table: str
) -> str:
    """
    Function exports a schema from Unity Catalog to be used by the Excel
    input form

    :param dataset: name of the schema in Unity Catalog
    :param spark: SparkSession object
    :param table: name of the table in Unity Catalog
    :return: schema_json: JSON string with the schema of the required dataset
    """

    if table:
        table_name_list = table
    else:
        table_name_list = get_table_name_list_from_unity_catalog(
            dataset=dataset, spark=spark
        )

    df_columns_tables = create_dataframe_containing_all_column_names_in_tables(
        table_name_list=table_name_list, spark=spark
    )

    list_of_all_table_name_to_column_names_mappings = (
        get_all_table_name_to_column_names_mappings(
            table_name_list=table_name_list, df_columns_tables=df_columns_tables
        )
    )

    return json.dumps(
        {
            "dataset": dataset,
            "tables": list_of_all_table_name_to_column_names_mappings,
        }
    )


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
            url = table["validate_table_schema_url"]
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
                            parameters={"column": column, "type_": rule_type},
                        )
                        table["rules"].append(rule)

    return dq_rules_dict


def data_quality_rules_json_string_to_dict(
    json_string: str,
) -> DataQualityRulesDict:
    """
    Function adds a mandatory line in case of a conditional rule

    :param json_string: A JSON string with all DQ configuration.
    :return: rule_json: A dictionary with all DQ configuration.
    """
    dq_rules_dict: DataQualityRulesDict = json.loads(json_string)

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
