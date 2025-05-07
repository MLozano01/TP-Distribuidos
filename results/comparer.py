#!/usr/bin/env python3

import json
import math
import sys

def read_json(path):
    try:
        with open(path, 'r') as file:
            datos = json.load(file)
        return datos
    except FileNotFoundError:
        print(f"Error: The file '{path}' wasn't found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: The file '{path}' isn't a valid JSON.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def find_item(my_list, condition):
    for item in my_list:
        if condition(item):
            return item
    return None

def main():
    if len(sys.argv) != 3:
        print("Usage: python comparer.py <base_file> <file_to_check>")
        sys.exit(1)

    base_data = read_json(sys.argv[1])
    check_data = read_json(sys.argv[2])
    if not base_data or not check_data:
        return
    
    for i in range(5):
      base = base_data[i]
      check = check_data[i]
      print(f"Checking query: {i + 1}")

      check_rows = check.get("result_row", [])
      base_rows = base.get("result_row", [])
      diff_found = False
      base_rows_checked = set()
      for check_row in check_rows:
          iter_check = iter(check_row)
          key = next(iter_check, None)
          value = next(iter_check, None)

          check_key = check_row.get(key)
          check_value = check_row.get(value)

          # Find the matching row in base
          base_match = next(
              (row for row in base_rows if row.get(key) == check_key), None
          )
          if not base_match:
              print(f"   - Key '{check_key}' not found in base")
              diff_found = True
          else:
                base_key = base_match.get(key)
                base_value = base_match.get(value)
                base_rows_checked.add(base_key)
                are_the_same = False
                try:
                    base_value_typed = float(base_value)
                    check_value_typed = float(check_value)
                    are_the_same = math.isclose(base_value_typed, check_value_typed, rel_tol=1e-6)
                except:
                    are_the_same = base_value == check_value
                finally:
                    if not are_the_same:
                        print(f"   - Mismatch for key '{check_key}': '{base_value_typed}' (base) vs '{check_value_typed}' (checking)")
                        diff_found = True

      if len(base_rows_checked) != len(base_rows):
        diff_found = True
        for base_row in base_rows:
          key = next(iter(base_row), None)
          base_key = base_row.get(key)
          if base_key in base_rows_checked:
            continue
          
          print(f"   - Key '{base_key}' not found in check")

      if not diff_found:
          print("   - No differences found")

         
    
if __name__ == "__main__":
    main()