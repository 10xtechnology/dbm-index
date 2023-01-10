from __future__ import annotations

from typing import Optional, Union, Dict, List, Literal


JsonDict = Dict[str, 'JsonType'] # type: ignore
JsonType = Optional[Union[JsonDict, List['JsonType'], int, float, str, bool]] # type: ignore