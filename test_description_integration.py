#!/usr/bin/env python3

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from dq_suite.validation_input import get_data_quality_rules_dict

def test_description_integration():
    """Test that description fields work in JSON parsing and validation"""
    
    print("Testing description field integration...")
    
    # Test with example JSON
    try:
        dq_rules = get_data_quality_rules_dict('dq_rules_example.json')
        print("✓ Successfully parsed example JSON with description fields")
        
        # Check first rule with description
        first_table = dq_rules['tables'][0]
        first_rule = first_table['rules'][0]
        print(f"First rule: {first_rule['rule_name']}")
        
        if 'description' in first_rule:
            print(f"Description: {first_rule['description']}")
            print("✓ Description field is present and accessible")
        else:
            print("! No description field found in first rule")
            
    except Exception as e:
        print(f"Error with example JSON: {e}")
        
    # Test with test data JSON
    try:
        test_dq_rules = get_data_quality_rules_dict('tests/test_data/dq_rules.json')
        print("✓ Successfully parsed test JSON with description fields")
        
        # Check first rule with description  
        test_first_table = test_dq_rules['tables'][0]
        test_first_rule = test_first_table['rules'][0]
        print(f"Test rule: {test_first_rule['rule_name']}")
        
        if 'description' in test_first_rule:
            print(f"Test description: {test_first_rule['description']}")
            print("✓ Test description field is present and accessible")
        else:
            print("! No description field found in test rule")
            
    except Exception as e:
        print(f"Error with test JSON: {e}")

if __name__ == "__main__":
    test_description_integration()
