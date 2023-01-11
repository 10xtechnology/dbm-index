from hashlib import sha256

from .types import JsonType


def custom_hash(val: str):
    return sha256(val.encode()).hexdigest()

def parse_comparable_json(x: JsonType):
    if not x:
        return 0
    elif isinstance(x, dict) or isinstance(x, list) :
        return len(x)
    elif isinstance(x, int) or isinstance(x, float):
        return x 
    elif isinstance(x, str):
        return float(x) if x.isnumeric() else len(x)
    else:
        return 1 if x else 0