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
                {'$project': {'_id': 0, 'x': '$paths.x', 'y': '$paths.y'}},
                {'$out': output_collection},
            ],
            allowDiskUse=True,
        )
