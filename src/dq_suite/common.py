import json
from dataclasses import dataclass
from typing import Any, Dict, List, Literal

import requests
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


@dataclass()
class Rule:
    """
    Groups the name of the GX validation rule together with the
    parameters required to apply this rule.
    """

    rule_name: str  # Name of the GX expectation
    parameters: List[Dict[str, Any]]  # Collection of parameters required for
    # evaluating the expectation

    def __getitem__(self, key) -> str | List[Dict[str, Any]] | None:
        if key == "rule_name":
            return self.rule_name
        elif key == "parameters":
            return self.parameters
        raise KeyError(key)


@dataclass()
class RulesDict:
    """
    Groups a list of Rule-objects together with the name of the table
    these rules are to be applied to, as well as a unique identifier used for
    identifying outliers.
    """

    unique_identifier: str  # TODO: List[str] for more complex keys?
    table_name: str
    rules_list: List[Rule]

    def __getitem__(self, key) -> str | List[Rule] | None:
        if key == "unique_identifier":
            return self.unique_identifier
        elif key == "table_name":
            return self.table_name
        elif key == "rules_list":
            return self.rules_list
        raise KeyError(key)


RulesDictList = List[RulesDict]  # a list of dictionaries containing DQ rules


@dataclass()
class DataQualityRulesDict:
    tables: RulesDictList

    def __getitem__(self, key) -> RulesDictList | None:
        if key == "tables":
            return self.tables
        raise KeyError(key)


@dataclass
class Validation:
    batch_request: RuntimeBatchRequest
    expectation_suite_name: str


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
) -> (Dict)[str, Any]:
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


def get_full_table_name(
    catalog_name: str, table_name: str, schema_name: str = "dataquality"
) -> str:
    return f"{catalog_name}.{schema_name}.{table_name}"


def write_to_unity_catalog(
    df: DataFrame,
    catalog_name: str,
    table_name: str,
    # schema: StructType,
    mode: Literal["append", "overwrite"] = "append",
) -> None:
    # TODO: enforce schema?
    # df = enforce_schema(df=df, schema_to_enforce=schema)
    full_table_name = get_full_table_name(
        catalog_name=catalog_name, table_name=table_name
    )
    df.write.mode(mode).option("overwriteSchema", "true").saveAsTable(
        full_table_name
    )  # TODO: write as delta-table? .format("delta")
