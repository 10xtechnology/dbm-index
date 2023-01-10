from __future__ import annotations

from json import dumps, loads
from typing import Optional, Union, Dict, List

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
        resource_head_encoded = self.db.get('head', b'-1')
        resource_head = int(resource_head_encoded.decode())
        resource_id = str(resource_head + 1)

        if resource_head >= 0:
            self.db[self.key_delim.join([resource_id, 'next'])] = resource_head_encoded

        self.db[self.key_delim.join([resource_id, 'head'])] = str(len(resource) - 1).encode()

        resource_id_encoded = resource_id.encode()
        self.db['head'] = resource_id_encoded

        for index, (key, value) in enumerate(resource.items()):
            key_id = str(index)
            self.db[self.key_delim.join([resource_id, key_id, 'value'])] = key.encode()

            if index > 0:
                self.db[self.key_delim.join([resource_id, key_id, 'next'])] = str(index - 1).encode()
            
            value_dump = dumps(value)
            key_hash = custom_hash(key)
            value_hash = custom_hash(value_dump)
            encoded_value_dump = value_dump.encode()

            self.db[self.key_delim.join([resource_id, key_hash])] = encoded_value_dump

            value_index_head_key = self.key_delim.join([key_hash, value_hash, 'head'])
            value_index_head_encoded = self.db.get(value_index_head_key, b'-1')
            value_index_head = int(value_index_head_encoded.decode())

            value_index_id = str(value_index_head + 1)
            self.db[self.key_delim.join([key_hash, value_hash, value_index_id])] = resource_id_encoded

            if value_index_head >= 0:
                self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = value_index_head_encoded

            self.db[value_index_head_key] = value_index_id.encode()

            key_head_key = self.key_delim.join([key_hash, 'head'])
            key_head = self.db.get(key_head_key)

            if not key_head:
                self.db[key_head_key] = encoded_value_dump
                continue

            leading_value = loads(key_head.decode())
            lagging_value = None 

            while parse_comparable_json(leading_value) < parse_comparable_json(value):
                lagging_value = leading_value
                leading_value_encoded = self.db.get(self.key_delim.join([key_hash, value_hash, 'next']))
                if not leading_value_encoded:
                    break 
                leading_value = loads(leading_value_encoded.decode())

            if leading_value_encoded:
                self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = leading_value_encoded
            
            if lagging_value:
                self.db[self.key_delim.join([key_hash, custom_hash(dumps(lagging_value)), 'next'])] = encoded_value_dump

        
        return resource_id

    def retrieve(self, filters: JsonDict = {}, keys: Optional[List[str]] = None, skip: int = 0, take: int = 10, sort_key: Optional[str] = None, sort_dir_asc: bool = True):
        if not filters:
            head = self.db.get('head')

            if not head:
                return [] 
            
            current_resource_id = head.decode()

            if not keys:
                key_head = self.db.get(self.key_delim.join([current_resource_id, 'keys', 'head']))
                

            while next_id := self.db.get(self.key_delim.join([current_resource_id, 'next'])):
                current_resource_id = next_id.decode()
