from __future__ import annotations

from json import dumps, loads
from typing import Optional, Union, Dict, List, Literal

from .helpers import custom_hash


JsonDict = Dict[str, 'JsonType'] # type: ignore
JsonType = Optional[Union[JsonDict, List['JsonType'], int, float, str, bool]] # type: ignore

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



class Pyomi:
    def __init__(self, db, key_delim='#'):
        self.db = db 
        self.key_delim = key_delim

    def create(self, resource: JsonDict) -> str:
        head_encoded = self.db.get('head', b'-1')
        head = int(head_encoded.decode())
        new_head = str(head + 1)

        if head >= 0:
            self.db[self.key_delim.join([new_head, 'next'])] = head_encoded

        self.db[self.key_delim.join([new_head, 'head'])] = str(len(resource) - 1).encode()

        new_head_encoded = new_head.encode()
        self.db['head'] = new_head_encoded

        for index, (key, value) in enumerate(resource.items()):
            resource_key_head = str(index)
            self.db[self.key_delim.join([new_head, resource_key_head, 'value'])] = key.encode()

            if index > 0:
                self.db[self.key_delim.join([new_head, resource_key_head, 'next'])] = str(index - 1).encode()
            
            value_dump = dumps(value)
            encoded_value_dump = value_dump.encode()

            key_hash = custom_hash(key)
            value_hash = custom_hash(value_dump)

            self.db[self.key_delim.join([new_head, key_hash])] = encoded_value_dump

            key_value_head_key = self.key_delim.join([key_hash, value_hash, 'head'])
            key_value_head_encoded = self.db.get(key_value_head_key, b'-1')
            key_value_head = int(key_value_head_encoded.decode())

            key_value_new_head = str(key_value_head + 1)
            self.db[self.key_delim.join([key_hash, value_hash, key_value_new_head])] = new_head_encoded

            if key_value_head >= 0:
                self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = key_value_head_encoded

            self.db[key_value_head_key] = key_value_new_head.encode()

            key_head_key = self.key_delim.join([key_hash, 'head'])
            key_head_encoded = self.db.get(key_head_key)

            if not key_head_encoded:
                self.db[key_head_key] = encoded_value_dump
                continue

            key_head = loads(key_head_encoded.decode())

            comparable_value = parse_comparable_json(value)

            if comparable_value > parse_comparable_json(key_head):
                self.db[key_head_key] = encoded_value_dump
                self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = key_head_encoded
            else:
                leading_value_encoded = key_head_encoded
                while leading_value_encoded:
                    leading_value = loads(leading_value_encoded.decode())

                    if comparable_value > parse_comparable_json(leading_value):
                        break

                    lagging_value = leading_value
                    leading_value_encoded = self.db.get(self.key_delim.join([key_hash, custom_hash(leading_value), 'next']))
                    leading_value = loads(leading_value_encoded.decode())

                if leading_value_encoded:
                    self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = leading_value_encoded
                
                self.db[self.key_delim.join([key_hash, custom_hash(dumps(lagging_value)), 'next'])] = encoded_value_dump

        
        return new_head

    def retrieve(self, filters: JsonDict = {}, keys: Optional[List[str]] = None, offset: int = 0, limit: int = 10, sort_key: Optional[str] = None, sort_direction: Literal['asc', 'desc'] = 'asc'):
        if not filters:
            head = self.db.get('head')

            if not head:
                return [] 
            
            current_resource_id = head.decode()

            if not keys:
                key_head = self.db.get(self.key_delim.join([current_resource_id, 'keys', 'head']))
                

            while next_id := self.db.get(self.key_delim.join([current_resource_id, 'next'])):
                current_resource_id = next_id.decode()
