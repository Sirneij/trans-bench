import csv
import logging
from typing import Any

from pymongo import ASCENDING, errors
from pymongo.database import Database

from common import Base

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class MongoDBOperations(Base):
    def __init__(self, config: dict[str, Any], db: Database) -> None:
        super().__init__(config)
        self.db = db

    def create_collection(
        self, collection_name: str, output_collection_name: str
    ) -> None:
        self.db[collection_name].drop()
        self.db.create_collection(collection_name)
        self.db[output_collection_name].drop()
        self.db.create_collection(output_collection_name)

    def insert_data(self, collection_name, data_file, chunk_size=1000):
        collection = self.db[collection_name]
        try:
            with open(data_file, 'r') as f:
                reader = csv.reader(f, delimiter='\t')
                collection.insert_many(
                    [{'x': int(row[0]), 'y': int(row[1])} for row in reader],
                    ordered=False,
                )
                # for row in reader:
                #     batch.append({'x': int(row[0]), 'y': int(row[1])})
                #     if len(batch) == chunk_size:
                #         collection.insert_many(batch, ordered=False)
                #         batch = []
                # if batch:
                #     collection.insert_many(batch, ordered=False)
        except (errors.BulkWriteError, errors.PyMongoError) as e:
            logging.error(f"An error occurred: {e}")
        except FileNotFoundError:
            logging.error(f"File {data_file} not found.")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

    def create_index(self, collection_name):
        collection = self.db[collection_name]
        collection.create_index([('x', ASCENDING), ('y', ASCENDING)], background=True)

    def export_to_csv(self, collection_name, output_file):
        collection = self.db[collection_name]
        cursor = collection.find({}, {'_id': 0})
        try:
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['x', 'y'])
                writer.writerow([[document['x'], document['y']] for document in cursor])

        except Exception as e:
            logging.error(f"An error occurred while writing to CSV: {e}")
