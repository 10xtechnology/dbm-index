from typing import Literal, List
from dataclasses import dataclass

from .types import JsonType


@dataclass
class Filter:
    key: str
    value: JsonType
    operator: Literal["lt", "le", "eq", "ne", "ge", "gt"] = "eq"
