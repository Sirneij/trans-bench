# mongodb_rules is registered in sys.modules by the connector at run time (engine/connectors/), so
# pylint cannot resolve it statically
# pylint: disable=import-error
from mongodb_rules import MongoDBOperations


class MongoDBRightRecursion(MongoDBOperations):
    def recursive_query(self, input_collection, output_collection):
        """Compute the transitive closure with $graphLookup (right recursion) into the output collection."""
        self.db[input_collection].aggregate(
            [
                {
                    '$graphLookup': {
                        'from': input_collection,
                        'startWith': '$x',
                        'connectFromField': 'y',
                        'connectToField': 'x',
                        'as': 'paths',
                        'restrictSearchWithMatch': {},
                    }
                },
                {'$unwind': '$paths'},
                {
                    '$project': {
                        '_id': 0,
                        'x': '$x',
                        'y': '$paths.y',
                    }
                },
                {
                    '$group': {
                        '_id': {'x': '$x', 'y': '$y'},
                        'x': {'$first': '$x'},
                        'y': {'$first': '$y'},
                    }
                },
                {'$project': {'_id': 0, 'x': 1, 'y': 1}},
                {'$out': output_collection},
            ],
            allowDiskUse=True,
        )
