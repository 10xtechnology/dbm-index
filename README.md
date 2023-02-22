# DBM Index

An object mapper and indexer for key value databases, written in python.

## Usage

```python
from dbm_index import Indexer

indexer = Indexer({})

resource_id = indexer.create({'hello': 'world'})

resource = indexer.retreive_one(resource_id)
resources = indexer.retreive()

indexer.update(resource_id, {'hello': 123})

indexer.delete(resource_id)
```