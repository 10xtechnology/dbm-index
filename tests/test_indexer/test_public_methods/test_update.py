from unittest import TestCase

from src.dbm_indexer import Indexer


class TestIndexerUpdate(TestCase):
    def test_update(self):
        indexer = Indexer({})

        resource_id = indexer.create({
            'hello': 'world',
            'test': 123
        })

        l1 = len(indexer.db)

        indexer.update(resource_id, {'test': 321})
        resource = indexer.retrieve_one(resource_id)

        self.assertEqual(resource['test'], 321)
        self.assertEqual(l1, len(indexer.db))