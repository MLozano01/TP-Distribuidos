from datetime import datetime


def to_float(value) -> float:
  try:
    return float(value)
  except:
    return -1.0

def to_int(value) -> int:
  try:
    return int(value)
  except:
    return -1

def to_string(value) -> str:
  value = value.replace("\\", "")
  return value.strip(' "\\') if value else ''

def to_bool(value) -> bool:
  return value == 'True'


def create_data_list(string_data):
  string_data = string_data.replace('[', "")
  string_data = string_data.replace(']', "")
  data_list = string_data.split('}')
  list_values = []
  for data in data_list:
    data = data.replace('{', "")
    values = data.split(",")
    dict_value = dict()
    for val in values:
      key_value = val.split(':')
      if len(key_value) != 2:
        continue
      key = remove_quotes(key_value[0])
      value = remove_quotes(key_value[1])
      dict_value[key.strip()] = value
    list_values.append(dict_value)

  return list_values

def remove_quotes(data):
  new_data = data.strip(" '\\")
  return new_data

def is_date(str_data):
  try:
    datetime.strptime(str_data, '%Y-%m-%d')
    return True
  except ValueError as e:
    return False