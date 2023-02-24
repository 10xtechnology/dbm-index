from unittest import TestCase

from src.dbm_indexer import Indexer


class TestIndexerDelete(TestCase):
    def test_delete(self): # TODO MULTIPLE
        indexer = Indexer({})

        resource_id = indexer.create({
            'hello': 'world',
            'test': 123
        })

        indexer.delete(resource_id)
        self.assertEqual(len(indexer.db), 0)
