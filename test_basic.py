#!/usr/bin/env python3
"""
Basic tests for the data processing pipeline
"""

import os
import sys
import json

def test_python_files_syntax():
    """Test that all Python files have valid syntax"""
    python_files = ['engineer_data_panda.py', 'upload_to_postgres_heroku.py', 'upload_to_postgres.py']
    
    for file in python_files:
        if os.path.exists(file):
            try:
                with open(file, 'r') as f:
                    compile(f.read(), file, 'exec')
                print(f"✓ {file} syntax is valid")
            except SyntaxError as e:
                print(f"✗ {file} has syntax error: {e}")
                return False
        else:
            print(f"⚠ {file} not found")
    
    return True

def test_sample_json_structure():
    """Test that sample JSON files have expected structure"""
    mixed_dir = 'mixed'
    if not os.path.exists(mixed_dir):
        print(f"⚠ {mixed_dir} directory not found")
        return True
    
    # Test first JSON file found
    json_files = [f for f in os.listdir(mixed_dir) if f.endswith('.json')]
    if not json_files:
        print("⚠ No JSON files found in mixed directory")
        return True
    
    test_file = os.path.join(mixed_dir, json_files[0])
    try:
        with open(test_file, 'r') as f:
            data = json.load(f)
        
        required_fields = ['user_id', 'email', 'advocacy_programs']
        for field in required_fields:
            if field not in data:
                print(f"✗ Missing required field: {field}")
                return False
        
        print(f"✓ JSON structure is valid (tested {test_file})")
        return True
        
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in {test_file}: {e}")
        return False
    except Exception as e:
        print(f"✗ Error reading {test_file}: {e}")
        return False

if __name__ == "__main__":
    print("Running basic tests...")
    
    tests = [
        test_python_files_syntax,
        test_sample_json_structure
    ]
    
    all_passed = True
    for test in tests:
        if not test():
            all_passed = False
    
    if all_passed:
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)