from mongodb_rules import MongoDBOperations


class MongoDBDoubleRecursion(MongoDBOperations):
    def recursive_query(self, input_collection, output_collection):
        self.db[input_collection].aggregate(
            [
                {
                    '$graphLookup': {
                        'from': input_collection,
                        'startWith': '$x',
                        'connectFromField': 'x',
                        'connectToField': 'y',
                        'as': 'left_paths',
                        'restrictSearchWithMatch': {},
                    }
                },
                {
                    '$graphLookup': {
                        'from': input_collection,
                        'startWith': '$y',
                        'connectFromField': 'y',
                        'connectToField': 'x',
                        'as': 'right_paths',
                        'restrictSearchWithMatch': {},
                    }
                },
                {'$unwind': '$left_paths'},
                {'$unwind': '$right_paths'},
                {
                    '$group': {
                        '_id': {'x': '$left_paths.x', 'y': '$right_paths.y'},
                        'x': {'$first': '$left_paths.x'},
                        'y': {'$first': '$right_paths.y'},
                    }
                },
                {'$project': {'_id': 0, 'x': 1, 'y': 1}},
                {'$out': output_collection},
            ],
            allowDiskUse=True,
        )
