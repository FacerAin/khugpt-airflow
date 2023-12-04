from pymongo import MongoClient

from dags.modules.utils import singleton


@singleton
class PymongoClient:
    def __init__(self, host="localhost", port=27017):
        self.client = self.get_client_connection(host, port)
        self.collection = self.client["khugpt"]["documents"]

    def get_client_connection(self, host, port):
        try:
            client = MongoClient(host=host, port=port)
        except Exception as e:
            raise f"Can't connect to mongodb host:{host} port:{port}"
        return client

    def get_collection(self):
        return self.collection

    def insert_documents(self, documents):
        return self.collection.insert_many(documents)
