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
    for table in rule_json["tables"]:
        for rule in table["rules"]:
            for parameter in rule["parameters"]:
                if "row_condition" in parameter:
                    # GX requires this statement for conditional rules when using spark
                    parameter["condition_parser"] = "great_expectations__experimental__"

    return rule_json
