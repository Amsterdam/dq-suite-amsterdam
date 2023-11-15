import json
# from GX import df_check

def handle_dq_error(e):
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

def handle_unexpected_error(e):
    print(f"An unexpected error occurred: {e}")

def handle_errors(dq_rules):
    try:
        rule_json = json.loads(dq_rules)
    except json.JSONDecodeError as e:
        handle_dq_error(e)
    except Exception as e:
        handle_unexpected_error(e)
    
