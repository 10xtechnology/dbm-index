from unittest import TestCase

from src.dbm_index import Indexer


class TestIndexerUpdate(TestCase):
    def test_update(self):  # TODO MULTIPLE
        indexer = Indexer({})

        resource_id = indexer.create({"hello": "world", "test": 123})

        l1 = len(indexer.db)

        indexer.update(resource_id, {"test": 321})
        resource = indexer.retrieve_one(resource_id)

        self.assertEqual(resource["test"], 321)
        self.assertEqual(l1, len(indexer.db))

    def test_update_with_new_keys(self):
        indexer = Indexer({})

        resource_id = indexer.create({'hello': 'world'})

        indexer.update(resource_id, {'test': 123})

        resource = indexer.retrieve_one(resource_id)

        assert resource['hello'] == 'world'
        assert resource['test'] == 123