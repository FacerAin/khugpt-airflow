from pymongo import MongoClient

def singleton(cls):
    instances = {}

    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper

@singleton
class PymongoClient:
  db = None
  client = None
  
  def __init__(self, host="localhost", port=27017):
    self.client = MongoClient(host=host, port=port)
    self.collection = self.client['khugpt']['documents']
    
  def get_collection(self):
    return self.collection
  
  def insert_documents(self, documents):
    return self.collection.insert_many(documents)
  
