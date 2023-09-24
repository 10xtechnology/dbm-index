from hashlib import sha256
from json import dumps

from .types import JsonType


def custom_hash(val: str):
    return sha256(val.encode()).hexdigest()


def lexographic_id(s: str):
    return int(f"{sum(map(ord, s))}{str(int(custom_hash(s), 16))}")


def parse_comparable_json(x: JsonType) -> JsonType:
    if x is None:
        return 0
    if isinstance(x, dict) or isinstance(x, list):
        x = dumps(x)
    if isinstance(x, str):
        return lexographic_id(x)
    if isinstance(x, bool):
        return 1 if x else 0
    return x
