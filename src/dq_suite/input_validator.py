import json


def validate_dqrules(dq_rules):
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
    for table in rule_json["dataframe_parameters"]:
        for rule in table["rules"]:
            for parameter in rule["parameters"]:
                if "row_condition" in parameter:
                    # GX requires this statement for conditional rules when using spark
                    parameter["condition_parser"] = "great_expectations__experimental__"

    return rule_json

def generate_dq_rules_from_schema(dq_rules: dict, schema: dict) -> dict:
    if not schema.get('schema') or not schema['schema'].get('properties'):
        raise KeyError("The provided schema dictionary does not have the expected structure.")

    schema_columns = schema['schema']['properties']
    if "schema" in schema_columns: del schema_columns["schema"]
    for table in dq_rules['dataframe_parameters']:
        if 'schema_id' in table and table['schema_id'] == schema['id']:
            for column, properties in schema_columns.items():
                column_type = properties.get('type')
                if column_type:
                    rule = {
                        "rule_name": "expect_column_values_to_be_of_type",
                        "parameters": [
                            {
                                "column": column,
                                "type_": column_type.capitalize() + "Type",
                                "result_format" : {"result_format": "COMPLETE"}
                            }
                        ]
                    }
                    table['rules'].append(rule)
    
    return dq_rules
