from __future__ import annotations

from json import dumps, loads
from typing import Optional, List, Literal
from operator import lt, gt

import operator

from .helpers import custom_hash, parse_comparable_json
from .types import JsonDict, JsonType
from .schemas import Filter



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
            self._create_filter_index(key_hash, value_hash, resource_id_encoded, encoded_value_dump, value)
        
        return resource_id

    def retrieve(self,
        filters: List[Filter] = [], 
        keys: Optional[List[str]] = None, 
        offset: int = 0, 
        limit: int = 10, 
        sort_key: Optional[str] = None, 
        sort_direction: Literal['asc', 'desc'] = 'asc'
    ):
        resources: List[dict] = []
        if limit <= 0:
            return resources

        if sort_key:
            sort_key_hash = custom_hash(sort_key)

            start_prop = 'head' if sort_direction == 'desc' else 'toe'
            link_prop = 'next' if sort_direction == 'desc' else 'prev'

            value_encoded = self.db.get(self.key_delim.join([sort_key_hash, start_prop]))

            while (offset > 0 or limit > 0) and value_encoded:
                value = value_encoded.decode()
                value_hash = custom_hash(value)
                key_value_id_encoded = self.db.get(self.key_delim.join([sort_key_hash, value_hash, 'head']))

                while (offset > 0 or limit > 0) and key_value_id_encoded:
                    key_value_id = key_value_id_encoded.decode()
                    resource_id = self.db.get(self.key_delim.join([sort_key_hash, value_hash, key_value_id, 'value'])).decode()

                    if (retrieved_values := self._check_filters(resource_id, filters)) is not None:
                        if offset > 0:
                            offset -= 1
                        else:
                            limit -= 1
                            resources.append(self._retrieve_resource(resource_id, keys, retrieved_values))
                    
                    key_value_id_encoded = self.db.get(self.key_delim.join([sort_key_hash, value_hash, key_value_id, 'next']))
                
                value_encoded = self.db.get(self.key_delim.join([sort_key_hash, value_hash, link_prop]))

            return resources
        
        else:
            resource_id_encoded = self.db.get('head')

            while (offset > 0 or limit > 0) and resource_id_encoded:
                resource_id = resource_id_encoded.decode()

                if (retrieved_values := self._check_filters(resource_id, filters)) is not None:
                    if offset > 0:
                        offset -= 1
                    else:
                        limit -= 1
                        resources.append(self._retrieve_resource(resource_id, keys, retrieved_values))  
                
                resource_id_encoded = self.db.get(self.key_delim.join([resource_id, 'next']))
        return resources

    def retrieve_one(self, resource_id: str, keys: Optional[List[str]] = None):
        return self._retrieve_resource(resource_id, keys=keys)

    def update(self, resource_id: str, update: JsonDict):
        resource_id_encoded = resource_id.encode()
        key_id_key = self.key_delim.join([resource_id, 'head'])

        while key_id_encoded := self.db.get(key_id_key):
            key_id = key_id_encoded.decode()
            key_id_key = self.key_delim.join([resource_id, key_id, 'next'])
            key = self._retrieve_key(resource_id, key_id)
            
            if not (new_value := update.get(key)):
                continue 

            key_hash = custom_hash(key)

            current_value_dump = self.db.get(self.key_delim.join([resource_id, key_hash])).decode()
            current_value_hash = custom_hash(current_value_dump)

            new_value_dump = dumps(new_value)

            new_value_hash = custom_hash(new_value_dump)
            encoded_new_value_dump = new_value_dump.encode()

            self._delete_filter_index(key_hash, current_value_hash, resource_id_encoded)
            self._create_filter_index(key_hash, new_value_hash, resource_id_encoded, encoded_new_value_dump, new_value)

            self.db[self.key_delim.join([resource_id, key_hash])] = encoded_new_value_dump
            
    def delete(self, resource_id: str):
        resource_id_encoded = resource_id.encode()
        key_id_key = self.key_delim.join([resource_id, 'head'])

        while key_id_encoded := self.db.get(key_id_key):
            key_id = key_id_encoded.decode()
            del self.db[key_id_key]
            
            key_id_key = self.key_delim.join([resource_id, key_id, 'next'])
            key = self._retrieve_key(resource_id, key_id)
            del self.db[self.key_delim.join([resource_id, key_id, 'value'])]

            key_hash = custom_hash(key)

            encoded_value_dump = self.db.get(self.key_delim.join([resource_id, key_hash]))
            value_dump = encoded_value_dump.decode()

            value_hash = custom_hash(value_dump)
            
            del self.db[self.key_delim.join([resource_id, key_hash])]

            self._delete_filter_index(key_hash, value_hash, resource_id_encoded)

        lagging_resource_id = None
        resource_id_key = 'head'

        while (retreived_resource_id_encoded := self.db.get(resource_id_key)):
            retreived_resource_id = retreived_resource_id_encoded.decode()
            next_retreived_resource_id_key = self.key_delim.join([retreived_resource_id, 'next'])
            

            if retreived_resource_id == resource_id:
                next_retreived_resource_id_encoded = self.db.get(next_retreived_resource_id_key)
                if lagging_resource_id:
                    if next_retreived_resource_id_encoded:
                        self.db[self.key_delim.join([lagging_resource_id, 'next'])] = next_retreived_resource_id_encoded
                        del self.db[next_retreived_resource_id_key]
                elif next_retreived_resource_id_encoded:                    
                    self.db[resource_id_key] = next_retreived_resource_id_encoded
                    del self.db[next_retreived_resource_id_key]
                else:
                    del self.db[resource_id_key]   

                return 

            resource_id_key = next_retreived_resource_id_key
            lagging_resource_id = retreived_resource_id


    def _check_filters(self, resource_id, filters: List[Filter] = []):
        retrieved_values = {}
        for f in filters:
            value = self._retrieve_value(resource_id, f.key)
            if not getattr(operator, f.operator)(parse_comparable_json(value), parse_comparable_json(f.value)):
                return
            retrieved_values[f.key] = value
        return retrieved_values

    def _retrieve_resource(
        self, 
        resource_id: str, 
        keys: Optional[List[str]] = None,
        retrieved_values: JsonDict = {}
    ):
        resource: JsonDict = {'id': resource_id}

        if keys:
            for key in keys:
                resource[key] = retrieved_values.get(key, self._retrieve_value(resource_id, key))

        else:
            key_id_encoded = self.db.get(self.key_delim.join([resource_id, 'head']))
            if not key_id_encoded:
                return resource
            key_id = key_id_encoded.decode()
            key = self._retrieve_key(resource_id, key_id)

            resource[key] = retrieved_values.get(key, self._retrieve_value(resource_id, key))

            while key_id_encoded := self.db.get(self.key_delim.join([resource_id, key_id, 'next'])):
                key_id = key_id_encoded.decode()
                key = self._retrieve_key(resource_id, key_id)
                resource[key] = retrieved_values.get(key, self._retrieve_value(resource_id, key))
        
        return resource

    def _retrieve_key(self, resource_id, key_id):
        return self.db.get(self.key_delim.join([resource_id, key_id, 'value'])).decode()

    def _retrieve_value(self, resource_id, key):
        key_hash = custom_hash(key)
        return loads(self.db.get(self.key_delim.join([resource_id, key_hash])).decode())


    def _resource_id(self):
        head_encoded = self.db.get('head', b'-1')
        head = int(head_encoded.decode())
        resource_id = str(head + 1)

        if head >= 0:
            self.db[self.key_delim.join([resource_id, 'next'])] = head_encoded
        else:
            self.db['head'] = resource_id.encode()

        return resource_id

    def _create_key_index(self, resource_id, key_index, key):
        resource_key_id = str(key_index)
        self.db[self.key_delim.join([resource_id, resource_key_id, 'value'])] = key.encode()

        if key_index > 0:
            self.db[self.key_delim.join([resource_id, resource_key_id, 'next'])] = str(key_index - 1).encode()

    def _create_filter_index(self, key_hash, value_hash, resource_id_encoded, encoded_value_dump, value):
        key_value_head_key = self.key_delim.join([key_hash, value_hash, 'head'])
        key_value_head_encoded = self.db.get(key_value_head_key, b'-1')
        key_value_head = int(key_value_head_encoded.decode())

        if key_value_head == -1:
            self._create_sort_index(key_hash, value_hash, encoded_value_dump, value)

        key_value_id = str(key_value_head + 1)
        self.db[self.key_delim.join([key_hash, value_hash, key_value_id, 'value'])] = resource_id_encoded

        if key_value_head >= 0:
            self.db[self.key_delim.join([key_hash, value_hash, key_value_id, 'next'])] = key_value_head_encoded

        self.db[key_value_head_key] = key_value_id.encode()

    def _delete_filter_index(self, key_hash, value_hash, resource_id_encoded):
        lagging_key_value_id = None
        key_value_id_key = self.key_delim.join([key_hash, value_hash, 'head'])
        

        while (key_value_id_encoded := self.db.get(key_value_id_key)):
            key_value_id = key_value_id_encoded.decode()
            resource_id_encoded_retreived_key = self.key_delim.join([key_hash, value_hash, key_value_id, 'value'])
            resource_id_encoded_retreived = self.db.get(resource_id_encoded_retreived_key)

            next_key_value_id_key = self.key_delim.join([key_hash, value_hash, key_value_id, 'next'])
            

            if resource_id_encoded_retreived == resource_id_encoded:
                next_key_value_id_encoded = self.db.get(next_key_value_id_key)
                if lagging_key_value_id:
                    if next_key_value_id_encoded:
                        self.db[self.key_delim.join([key_hash, value_hash, lagging_key_value_id, 'next'])] = next_key_value_id_encoded
                        del self.db[next_key_value_id_key]
                elif next_key_value_id_encoded:                    
                    self.db[key_value_id_key] = next_key_value_id_encoded
                    del self.db[next_key_value_id_key]
                else:
                    del self.db[key_value_id_key]
                    self._delete_sort_index(key_hash, value_hash)


                del self.db[self.key_delim.join([key_hash, value_hash, key_value_id, 'value'])]                

                return 

            key_value_id_key = next_key_value_id_key
            lagging_key_value_id = key_value_id

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
                leading_value_decoded = leading_value_encoded.decode()
                leading_value = loads(leading_value_decoded)

                if compare_func(comparable_value, parse_comparable_json(leading_value)):
                    break

                lagging_value = leading_value
                leading_value_encoded = self.db.get(self.key_delim.join([key_hash, custom_hash(leading_value_decoded), link_prop]))

            lagging_value_dump = dumps(lagging_value)
            
            if leading_value_encoded:
                self.db[self.key_delim.join([key_hash, value_hash, link_prop])] = leading_value_encoded
                self.db[self.key_delim.join([key_hash, custom_hash(leading_value_decoded), secondary_link_prop])] = encoded_value_dump
            
            self.db[self.key_delim.join([key_hash, value_hash, secondary_link_prop])] = lagging_value_dump.encode()
            self.db[self.key_delim.join([key_hash, custom_hash(lagging_value_dump), link_prop])] = encoded_value_dump

    def _delete_sort_index(self, key_hash, value_hash):
        prev_encoded_value_dump_key = self.key_delim.join([key_hash, value_hash, 'prev'])
        next_encoded_value_dump_key = self.key_delim.join([key_hash, value_hash, 'next'])

        prev_encoded_value_dump = self.db.get(prev_encoded_value_dump_key)
        next_encoded_value_dump = self.db.get(next_encoded_value_dump_key)

        head_key = self.key_delim.join([key_hash, 'head'])
        toe_key = self.key_delim.join([key_hash, 'toe'])

        if prev_encoded_value_dump and next_encoded_value_dump:
            prev_value_dump = prev_encoded_value_dump.decode()
            next_value_dump = next_encoded_value_dump.decode()

            prev_value_dump_hash = custom_hash(prev_value_dump)
            next_value_dump_hash = custom_hash(next_value_dump)

            self.db[self.key_delim.join([key_hash, prev_value_dump_hash, 'next'])] = next_encoded_value_dump
            self.db[self.key_delim.join([key_hash, next_value_dump_hash, 'prev'])] = prev_encoded_value_dump

            del self.db[prev_encoded_value_dump_key]
            del self.db[next_encoded_value_dump_key]

        elif prev_encoded_value_dump:
            self.db[head_key] = prev_encoded_value_dump
            del self.db[prev_encoded_value_dump_key]

        elif next_encoded_value_dump:
            self.db[toe_key] = prev_encoded_value_dump
            del self.db[next_encoded_value_dump_key]

        else:
            del self.db[toe_key]
            del self.db[head_key]
