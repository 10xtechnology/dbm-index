from unittest import TestCase
from random import randint
from json import dumps

from src.dbm_index import Indexer
from src.dbm_index.schemas import Filter
from src.dbm_index.helpers import custom_hash


class TestIndexerPrivateMethods(TestCase):
    def test_check_filters(self):  # TODO TEST FOR ALL OPERATORS AND TYPES
        indexer = Indexer({})
        resource = {"hello": "world"}
        resource_id = indexer.create(resource)
        filters = [Filter(key="hello", value="world")]
        retrieved_values = indexer._check_filters(resource_id, filters=filters)
        self.assertEqual(retrieved_values, resource)

    def test_retrieve_resource(self):  # TODO TEST WITH KEYS AND RETRIEVED_VALUES
        indexer = Indexer({})
        resource = {"hello": "world"}
        resource_id = indexer.create(resource)
        retrieved_resource = indexer._retrieve_resource(resource_id)
        self.assertEqual({**resource, "id": "0"}, retrieved_resource)

    def test_retrieve_key(self):
        indexer = Indexer({})
        resource = {"hello": "world"}
        resource_id = indexer.create(resource)
        key = indexer._retrieve_key(resource_id, "0")
        self.assertEqual(key, "hello")

    def test_retrieve_non_existant_key(self):
        indexer = Indexer({})
        resource_id = indexer.create({})
        value = indexer._retrieve_value(resource_id, "somerandomcrap")
        self.assertEqual(value, None)

    def test_retrieve_value(self):
        indexer = Indexer({})
        resource = {"hello": "world"}
        resource_id = indexer.create(resource)
        value = indexer._retrieve_value(resource_id, "hello")
        self.assertEqual(value, "world")

    def test_retrieve_value_non_existant_key(self):
        indexer = Indexer({})
        resource = {"hello": "world"}
        resource_id = indexer.create(resource)
        value = indexer._retrieve_value(resource_id, "somerandomcrap")
        self.assertEqual(value, None)

    def test_resource_id(self):
        indexer = Indexer({})
        self.assertEqual(indexer._resource_id(), "0")
        self.assertEqual(indexer._resource_id(), "1")

    def test_create_key_index(self):
        indexer = Indexer({})
        indexer._create_key_index("0", 0, "hello")
        indexer._create_key_index("0", 1, "world")
        self.assertEqual(
            indexer.db, {"0#0#value": b"hello", "0#1#value": b"world", "0#1#next": b"0"}
        )

    def test_create_filter_index(self):  # TODO MULTIPLE
        indexer = Indexer({})
        resource_id_encoded = b"0"
        key = "hello"
        value = "world"
        key_hash = custom_hash(key)
        value_hash = custom_hash(value)
        encoded_value_dump = dumps(value).encode()

        indexer._create_filter_index(
            key_hash, value_hash, resource_id_encoded, encoded_value_dump, value
        )

        self.assertEqual(
            indexer.db,
            {
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824#head": b'"world"',
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824#toe": b'"world"',
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824#486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7#0#value": b"0",
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824#486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7#head": b"0",
            },
        )

    def test_delete_filter_index(self):  # TODO MULTIPLE
        indexer = Indexer({})
        resource_id_encoded = b"0"
        key = "hello"
        value = "world"
        key_hash = custom_hash(key)
        value_hash = custom_hash(value)
        encoded_value_dump = dumps(value).encode()

        indexer._create_filter_index(
            key_hash, value_hash, resource_id_encoded, encoded_value_dump, value
        )
        indexer._delete_filter_index(key_hash, value_hash, resource_id_encoded)

        self.assertEqual(indexer.db, {})

    def test_create_sort_index(self):  # TODO MULTIPLE
        indexer = Indexer({})
        key = "hello"
        value = "world"
        key_hash = custom_hash(key)
        value_hash = custom_hash(value)
        encoded_value_dump = dumps(value).encode()

        indexer._create_sort_index(key_hash, value_hash, encoded_value_dump, value)

        self.assertEqual(
            indexer.db,
            {
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824#head": b'"world"',
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824#toe": b'"world"',
            },
        )

    def test_delete_sort_index(self):  # TODO MULTIPLE
        indexer = Indexer({})
        key = "hello"
        value = "world"
        key_hash = custom_hash(key)
        value_hash = custom_hash(value)
        encoded_value_dump = dumps(value).encode()

        indexer._create_sort_index(key_hash, value_hash, encoded_value_dump, value)
        indexer._delete_sort_index(key_hash, value_hash)

        self.assertEqual(indexer.db, {})
