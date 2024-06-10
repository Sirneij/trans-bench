from mongodb_rules import MongoDBOperations


class MongoDBRightRecursion(MongoDBOperations):
    def recursive_query(self, input_collection, output_collection):
        self.db[output_collection].drop()
        self.db.create_collection(output_collection)

        self.db[input_collection].aggregate(
            [
                {
                    '$graphLookup': {
                        'from': input_collection,
                        'startWith': '$y',
                        'connectFromField': 'y',
                        'connectToField': 'x',
                        'as': 'paths',
                        'restrictSearchWithMatch': {},
                    }
                },
                {'$unwind': '$paths'},
                {'$project': {'_id': 0, 'x': '$paths.x', 'y': '$paths.y'}},
                {'$out': output_collection},
            ]
        )
