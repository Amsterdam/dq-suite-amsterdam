import json

from pyspark.sql import SparkSession


def validate_dqrules(dq_rules):
    """
    Function validates the input JSON
    
    :param dq_rules: A string with all DQ configuration.
    :type dq_rules: str
    """

    try:
        rule_json = json.loads(dq_rules)

    except json.JSONDecodeError as e:
        error_message = str(e)
        print(f"Data quality check failed: {error_message}")
        if "Invalid control character at:" in error_message:
            print("Quota is missing in the JSON.")
        if "Expecting ',' delimiter:" in error_message:
            print("Square brackets, Comma or curly brackets can be missing in the JSON.")
        if "Expecting ':' delimiter:" in error_message:
            print("Colon is missing in the JSON.")
        if "Expecting value:" in error_message:
            print("Rules's Value is missing in the JSON.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def expand_input(rule_json):
    """
    Function adds a mandatory line in case of a conditional rule
    
    :param rule_json: A dictionary with all DQ configuration.
    :type rule_json: dict
    :return: rule_json: A dictionary with all DQ configuration.
    :rtype: dict
    """

    for table in rule_json["dataframe_parameters"]:
        for rule in table["rules"]:
            for parameter in rule["parameters"]:
                if "row_condition" in parameter:
                    # GX requires this statement for conditional rules when using spark
                    parameter["condition_parser"] = "great_expectations__experimental__"

    return rule_json


def export_schema(dataset: str, spark: SparkSession):
    """
    Function exports a schema from Unity Catalog to be used by the Excel input form
    
    :param dataset: The name of the required dataset
    :type dataset: str
    :param spark: The current SparkSession required for querying
    :type spark: SparkSession
    :return: schema_json: A JSON string with the schema of the required dataset
    :rtype: str
    """

    table_query = """
        SELECT table_name
        FROM system.information_schema.tables
        WHERE table_schema = {dataset}
    """
    tables = spark.sql(table_query, dataset=dataset).select('table_name').rdd.flatMap(lambda x: x).collect()
    table_list = "'" + "', '".join(tables) + "'" #creates a list of all tables, is used by the next query

    column_query = f"""
            SELECT column_name, table_name
            FROM system.information_schema.columns
            WHERE table_name IN ({table_list})
        """
    columns = spark.sql(column_query).select('column_name', 'table_name')

    columns_list = []
    for table in tables:
        columns_table = columns.filter(columns.table_name == table).select('column_name').rdd.flatMap(lambda x: x).collect()
        columns_dict = {
                "table_name": table,
                "attributes": columns_table
            }
        columns_list.append(columns_dict)

    output_dict = {
        "dataset": dataset,
        "tables": columns_list
    }
    
    return json.dumps(output_dict)
