import pinecone
from dags.modules.utils import singleton


@singleton
class PineconeClient:
  db = None
  client = None
  
  def __init__(self, api_key: str, environment: str):
    pinecone.init(api_key=api_key,
              environment=environment)
    # Get Index
  
