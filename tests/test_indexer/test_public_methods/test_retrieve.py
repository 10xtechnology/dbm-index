from unittest import TestCase

from src.dbm_index import Indexer
from src.dbm_index.schemas import Filter


class TestIndexerRetrieve(TestCase):
    def test_retrieve_all(self):
        indexer = Indexer({})

        indexer.create({"test": 1, "hello": True})
        indexer.create({"test": 2, "hello": False})
        indexer.create({"test": 3, "hello": True})

        response = indexer.retrieve()

        self.assertEqual(
            response,
            [
                {"id": "2", "hello": True, "test": 3},
                {"id": "1", "hello": False, "test": 2},
                {"id": "0", "hello": True, "test": 1},
            ],
        )

    def test_retrieve_filters(self):
        indexer = Indexer({})

        indexer.create({"test": 1})
        indexer.create({"test": 2})
        indexer.create({"test": 3})

        filters = [Filter(key="test", value=1, operator="gt")]

        response = indexer.retrieve(filters=filters)

        self.assertEqual(response, [{"id": "2", "test": 3}, {"id": "1", "test": 2}])

    def test_retrieve_keys(self):
        indexer = Indexer({})

        indexer.create({"test": 123, "hello": "world", "foo": "bar"})

        response = indexer.retrieve(keys=["hello", "foo"])

        self.assertEqual(response, [{"id": "0", "hello": "world", "foo": "bar"}])

    def test_retrieve_page(self):
        indexer = Indexer({})

        indexer.create({"test": 1, "hello": True})
        indexer.create({"test": 2, "hello": False})
        indexer.create({"test": 3, "hello": True})

        response = indexer.retrieve(limit=1, offset=1)
        self.assertEqual(response, [{"id": "1", "hello": False, "test": 2}])

    def test_retrieve_sort(self):
        indexer = Indexer({})

        indexer.create({"test": 3})
        indexer.create({"test": 1})
        indexer.create({"test": 2})

        response = indexer.retrieve(sort_key="test", sort_direction="asc")
        self.assertEqual(
            response,
            [{"id": "1", "test": 1}, {"id": "2", "test": 2}, {"id": "0", "test": 3}],
        )

    def test_retrieve_empty_db_with_filters(self):
        indexer = Indexer({})
        response = indexer.retrieve(filters=[Filter("test", 123)])
        self.assertEqual(response, [])
