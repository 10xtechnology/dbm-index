from __future__ import annotations

from json import dumps, loads
from typing import Optional, Literal, List
from operator import lt, gt

from .helpers import custom_hash, parse_comparable_json
from .types import JsonDict



class Indexer:
    def __init__(self, db, key_delim='#'):
        self.db = db 
        self.key_delim = key_delim

    def create(self, resource: JsonDict) -> str:
        resource_id = self._resource_id()

        self.db[self.key_delim.join([resource_id, 'head'])] = str(len(resource) - 1).encode()

        resource_id_encoded = resource_id.encode()
        self.db['head'] = resource_id_encoded

        for index, (key, value) in enumerate(resource.items()):
            value_dump = dumps(value)
            encoded_value_dump = value_dump.encode()

            key_hash = custom_hash(key)
            value_hash = custom_hash(value_dump)

            self.db[self.key_delim.join([resource_id, key_hash])] = encoded_value_dump

            self._create_key_index(resource_id, index, key)
            self._create_filter_index(key_hash, value_hash, resource_id_encoded)
            self._create_sort_index(key_hash, value_hash, encoded_value_dump, value)
        
        return resource_id

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



    def _resource_id(self):
        head_encoded = self.db.get('head', b'-1')
        head = int(head_encoded.decode())
        resource_id = str(head + 1)

        if head >= 0:
            self.db[self.key_delim.join([resource_id, 'next'])] = head_encoded
        
        return resource_id

    def _create_key_index(self, resource_id, key_index, key):
        resource_key_id = str(key_index)
        self.db[self.key_delim.join([resource_id, resource_key_id, 'value'])] = key.encode()

        if key_index > 0:
            self.db[self.key_delim.join([resource_id, resource_key_id, 'next'])] = str(index - 1).encode()

    def _create_filter_index(self, key_hash, value_hash, resource_id_encoded, ):
        key_value_head_key = self.key_delim.join([key_hash, value_hash, 'head'])
        key_value_head_encoded = self.db.get(key_value_head_key, b'-1')
        key_value_head = int(key_value_head_encoded.decode())

        key_value_id = str(key_value_head + 1)
        self.db[self.key_delim.join([key_hash, value_hash, key_value_id])] = resource_id_encoded

        if key_value_head >= 0:
            self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = key_value_head_encoded

        self.db[key_value_head_key] = key_value_id.encode()

    def _create_sort_index(self, key_hash, value_hash, encoded_value_dump, value):
        key_head_key = self.key_delim.join([key_hash, 'head'])
        key_toe_key = self.key_delim.join([key_hash, 'toe'])
        key_head_encoded = self.db.get(key_head_key)
        
        if not key_head_encoded:
            self.db[key_head_key] = encoded_value_dump
            self.db[key_toe_key] = encoded_value_dump
            return

        key_toe_encoded = self.db.get(key_toe_key) 
        
        key_toe_dump = key_toe_encoded.decode()
        key_head_dump = key_head_encoded.decode()

        key_toe = loads(key_toe_dump)
        key_head = loads(key_head_dump)

        comparable_value = parse_comparable_json(value)

        if (head_diff := (comparable_value - parse_comparable_json(key_head))) > 0:
            self.db[key_head_key] = encoded_value_dump
            self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = key_head_encoded
            self.db[self.key_delim.join([key_hash, custom_hash(key_head_dump), 'prev'])] = encoded_value_dump
        elif (toe_diff := (parse_comparable_json(key_toe) - comparable_value)) > 0:
            self.db[key_toe_key] = encoded_value_dump
            self.db[self.key_delim.join([key_hash, value_hash, 'prev'])] = key_toe_encoded
            self.db[self.key_delim.join([key_hash, custom_hash(key_toe_dump), 'next'])] = encoded_value_dump
        else:
            dir_flag = head_diff < toe_diff

            leading_value_encoded = key_head_encoded if dir_flag else key_toe_encoded
            compare_func = gt if dir_flag else lt
            link_prop, secondary_link_prop = ('next', 'prev') if dir_flag else ('prev', 'next')

            while leading_value_encoded:
                leading_value = loads(leading_value_encoded.decode())

                if compare_func(comparable_value, parse_comparable_json(leading_value)):
                    break

                lagging_value = leading_value
                leading_value_encoded = self.db.get(self.key_delim.join([key_hash, custom_hash(leading_value), link_prop]))
                leading_value = loads(leading_value_encoded.decode())

            lagging_value_dump = dumps(lagging_value)
            
            if leading_value_encoded:
                self.db[self.key_delim.join([key_hash, value_hash, link_prop])] = leading_value_encoded
                self.db[self.key_delim.join([key_hash, value_hash, secondary_link_prop])] = lagging_value_dump.encode()

            
            self.db[self.key_delim.join([key_hash, custom_hash(lagging_value_dump), link_prop])] = encoded_value_dump
