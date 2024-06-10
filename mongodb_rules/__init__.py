import csv
from pymongo import MongoClient, ASCENDING, errors
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s'
)


class MongoDBOperations:
    def __init__(self, uri, database):
        self.client = MongoClient(uri)
        self.db = self.client[database]

    def close(self):
        self.client.close()

    def create_collection(self, collection_name):
        self.db[collection_name].drop()
        self.db.create_collection(collection_name)

    def insert_data(self, collection_name, data_file, chunk_size=1000):
        collection = self.db[collection_name]
        try:
            with open(data_file, 'r') as f:
                reader = csv.reader(f, delimiter='\t')
                batch = []
                for row in reader:
                    batch.append({'x': int(row[0]), 'y': int(row[1])})
                    if len(batch) == chunk_size:
                        collection.insert_many(batch)
                        batch = []
                if batch:
                    collection.insert_many(batch)
        except (errors.BulkWriteError, errors.PyMongoError) as e:
            logging.error(f"An error occurred: {e}")
        except FileNotFoundError:
            logging.error(f"File {data_file} not found.")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

    def create_index(self, collection_name, field_name):
        collection = self.db[collection_name]
        collection.create_index([(field_name, ASCENDING)])

    def export_to_csv(self, collection_name, output_file):
        collection = self.db[collection_name]
        cursor = collection.find({}, {'_id': 0})
        try:
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['x', 'y'])
                for document in cursor:
                    writer.writerow([document['x'], document['y']])
        except Exception as e:
            logging.error(f"An error occurred while writing to CSV: {e}")
