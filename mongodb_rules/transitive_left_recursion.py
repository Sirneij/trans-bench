from mongodb_rules import MongoDBOperations


class MongoDBLeftRecursion(MongoDBOperations):
    def recursive_query(self, input_collection, output_collection):
        self.db[input_collection].aggregate(
            [
                {
                    '$graphLookup': {
                        'from': input_collection,
                        'startWith': '$x',
                        'connectFromField': 'x',
                        'connectToField': 'y',
                        'as': 'paths',
                        'restrictSearchWithMatch': {},
                    }
                },
                {'$unwind': '$paths'},
                {
                    '$group': {
                        '_id': {'x': '$paths.x', 'y': '$paths.y'},
                        'x': {'$first': '$paths.x'},
                        'y': {'$first': '$paths.y'},
                    }
                },
                {'$project': {'_id': 0, 'x': 1, 'y': 1}},
                {'$out': output_collection},
            ],
            allowDiskUse=True,
        )
