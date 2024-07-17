import json
import requests

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
    for table in rule_json["tables"]:
        for rule in table["rules"]:
            for parameter in rule["parameters"]:
                if "row_condition" in parameter:
                    # GX requires this statement for conditional rules when using spark
                    parameter["condition_parser"] = "great_expectations__experimental__"

    return rule_json

def fetch_schema_from_github(dq_rules):
    schemas = {}
    for table in dq_rules['tables']:
        if 'validate_table_schema_url' in table:
            url = table['validate_table_schema_url']
            r = requests.get(url)
            schema = json.loads(r.text)
            schemas[table['table_name']] = schema

    return schemas

def generate_dq_rules_from_schema(dq_rules: dict, schemas: dict) -> dict:
    for table in dq_rules['tables']:
        if 'validate_table_schema' in table:
            schema_id = table['validate_table_schema']
            table_name = table['table_name']
            
            if table_name in schemas:
                schema = schemas[table_name]
                if 'schema' in schema and 'properties' in schema['schema']: schema_columns = schema['schema']['properties']# separated tables - getting from table json    
                elif 'tables' in schema: # integrated tables - getting from dataset.json
                    for t in schema['tables']:
                        if t['id'] == schema_id:
                            schema_columns = t['schema']['properties']
                            break
                
                if "schema" in schema_columns: del schema_columns["schema"]

                for column, properties in schema_columns.items():
                    column_type = properties.get('type')
                    if column_type:
                        if column_type == 'number': rule_type = "IntegerType"
                        else: rule_type = column_type.capitalize() + "Type"
                        rule = {
                            "rule_name": "expect_column_values_to_be_of_type",
                            "parameters": [
                                {
                                    "column": column,
                                    "type_": rule_type
                                }
                            ]
                        }
                        table['rules'].append(rule)
    
    return dq_rules
