from unittest import TestCase
from random import randint
from json import dumps

from src.dbm_indexer import Indexer
from src.dbm_indexer.schemas import Filter

class TestIndexer(TestCase):
    def test_create(self):
        indexer = Indexer({})
        indexer.create({'test': 123})
        self.maxDiff = None
        
        self.assertEqual(indexer.db, {
            '0#head': b'0', 
            'head': b'0', 
            '0#0#value': b'test', 
            '0#9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08': b'123', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3#0#value': b'0', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3#head': b'0',
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#head': b'123',
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#toe': b'123'
        })

    def test_create_multiple(self):
        indexer = Indexer({})
        indexer.create({'test': 123})
        indexer.create({'test': 321})
        self.assertEqual(indexer.db, {
            '0#head': b'0', 
            'head': b'1', 
            '0#0#value': b'test', 
            '0#9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08': b'123', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3#0#value': b'0', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3#head': b'0', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#head': b'321', 
            '1#next': b'0', 
            '1#head': b'0', 
            '1#0#value': b'test', 
            '1#9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08': b'321', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#8d23cf6c86e834a7aa6eded54c26ce2bb2e74903538c61bdd5d2197997ab2f72#0#value': b'1', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#8d23cf6c86e834a7aa6eded54c26ce2bb2e74903538c61bdd5d2197997ab2f72#head': b'0', 
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#8d23cf6c86e834a7aa6eded54c26ce2bb2e74903538c61bdd5d2197997ab2f72#next': b'123',
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3#prev': b'321',
            '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08#toe': b'123'
        })

    def test_retrieve_all(self):
        indexer = Indexer({})

        indexer.create({'test': 1, 'hello': True})
        indexer.create({'test': 2, 'hello': False})
        indexer.create({'test': 3, 'hello': True})

        response = indexer.retrieve()

        self.assertEqual(response, [
            {'id': '2', 'hello': True, 'test': 3}, 
            {'id': '1', 'hello': False, 'test': 2}, 
            {'id': '0', 'hello': True, 'test': 1}
        ])
    
    def test_retrieve_filters(self):
        indexer = Indexer({})

        indexer.create({'test': 1})
        indexer.create({'test': 2})
        indexer.create({'test': 3})

        filters = [
            Filter(key='test', value=1, operator='gt')
        ]

        response = indexer.retrieve(filters=filters)
        
        self.assertEqual(response, [
            {'id': '2', 'test': 3}, 
            {'id': '1', 'test': 2}
        ])

    def test_retrieve_keys(self):
        indexer = Indexer({})

        indexer.create({
            'test': 123,
            'hello': 'world',
            'foo': 'bar'
        })

        response = indexer.retrieve(keys=['hello', 'foo'])
        
        self.assertEqual(response, [{'id': '0', 'hello': 'world', 'foo': 'bar'}])
    
    def test_retrieve_page(self):
        indexer = Indexer({})

        indexer.create({'test': 1, 'hello': True})
        indexer.create({'test': 2, 'hello': False})
        indexer.create({'test': 3, 'hello': True})

        response = indexer.retrieve(limit=1, offset=1)
        self.assertEqual(response, [{'id': '1', 'hello': False, 'test': 2}])

    def test_retrieve_sort(self):
        indexer = Indexer({})

        indexer.create({'test': 3})
        indexer.create({'test': 1})
        indexer.create({'test': 2})

        response = indexer.retrieve(sort_key='test', sort_direction='asc')
        self.assertEqual(response, [{'id': '1', 'test': 1}, {'id': '2', 'test': 2}, {'id': '0', 'test': 3}])

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

    def test_delete(self):
        indexer = Indexer({})

        resource_id = indexer.create({
            'hello': 'world',
            'test': 123
        })

        indexer.delete(resource_id)
        self.assertEqual(len(indexer.db), 0)
