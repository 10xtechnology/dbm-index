from hashlib import sha256
from json import dumps


def custom_hash(val: str):
    return sha256(val.encode()).hexdigest()


class Pyomi:
    def __init__(self, db):
        self.db = db 
        self.key_delim = '#'

    def create(self, resource):
        resource_head_encoded = self.db.get('head', b'-1')
        resource_head = int(resource_head_encoded.decode())
        resource_id = str(resource_head + 1)

        if resource_head >= 0:
            self.db[self.key_delim.join([resource_id, 'next'])] = resource_head_encoded

        
        
        self.db[self.key_delim.join([resource_id, 'key', 'head'])] = str(len(resource) - 1).encode()

        resource_id_encoded = resource_id.encode()
        self.db['head'] = resource_id_encoded

        for index, (key, value) in enumerate(resource.items()):
            key_id = str(index)
            self.db[self.key_delim.join([resource_id, key_id, 'value'])] = key.encode()

            if index > 0:
                self.db[self.key_delim.join([resource_id, key_id, 'next'])] = str(index - 1).encode()
            
            value = dumps(value)
            key_hash = custom_hash(key)
            value_hash = custom_hash(value)

            self.db[self.key_delim.join([resource_id, key_hash])] = value.encode()
            value_index_head_key = self.key_delim.join([key_hash, value_hash, 'head'])
            value_index_head_encoded = self.db.get(value_index_head_key, b'-1')
            value_index_head = int(value_index_head_encoded.decode())
            value_index_id = str(value_index_head + 1)
            self.db[self.key_delim.join([key_hash, value_hash, value_index_id])] = resource_id_encoded

            if value_index_head >= 0:
                self.db[self.key_delim.join([key_hash, value_hash, 'next'])] = value_index_head_encoded

            self.db[value_index_head_key] = value_index_id.encode()
        
        return resource_id