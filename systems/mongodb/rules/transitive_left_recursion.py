from mongodb_rules import MongoDBOperations


class MongoDBLeftRecursion(MongoDBOperations):
    def recursive_query(self, input_collection, output_collection):
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
