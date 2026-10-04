"""The steps of a MongoDB run; the rule files add the $graphLookup query (systems/mongodb/rules/)."""

import csv
import logging
from typing import Any

from pymongo import ASCENDING, errors
from pymongo.database import Database

from common import Base

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class MongoDBOperations(Base):
    """Create, load, index and export; the connector times each of these steps."""

    def __init__(self, config: dict[str, Any], db: Database) -> None:
        """Keep the configuration and the benchmark database."""
        super().__init__(config)
        self.db = db

    def create_collection(self, collection_name: str, output_collection_name: str) -> None:
        """Recreate the edge collection and the result collection, empty."""
        self.db[collection_name].drop()
        self.db.create_collection(collection_name)
        self.db[output_collection_name].drop()
        self.db.create_collection(output_collection_name)

    def insert_data(self, collection_name, data_file, chunk_size=1000):
        """Insert every edge of a tab-separated file as one document {x, y}, in one unordered batch."""
        collection = self.db[collection_name]
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t')
                collection.insert_many(
                    [{'x': int(row[0]), 'y': int(row[1])} for row in reader],
                    ordered=False,
                )
        except (errors.BulkWriteError, errors.PyMongoError) as e:
            logging.error(f"An error occurred: {e}")
        except FileNotFoundError:
            logging.error(f"File {data_file} not found.")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

    def create_index(self, collection_name):
        """Index the edges on (x, y)."""
        collection = self.db[collection_name]
        collection.create_index([('x', ASCENDING), ('y', ASCENDING)], background=True)

    def export_to_csv(self, collection_name, output_file):
        """Write the result collection as CSV, in batches of 1,000 documents."""
        collection = self.db[collection_name]
        cursor = collection.find({}, {'_id': 0}).batch_size(1000)

        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['x', 'y'])

                batch = []
                for document in cursor:
                    batch.append([document['x'], document['y']])
                    if len(batch) >= 1000:
                        writer.writerows(batch)
                        batch = []

                if batch:
                    writer.writerows(batch)

        except Exception as e:
            logging.error(f"An error occurred while writing to CSV: {e}")
