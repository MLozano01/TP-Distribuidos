def to_float(value) -> float:
  try:
    return float(value if value else -1.0)
  except:
    return -1.0

def to_int(value) -> int:
  try:
    return int(value if value else -1)
  except:
    return -1

def to_string(value) -> str:
  return value if value else ''

def to_bool(value) -> bool:
  return value == 'True'