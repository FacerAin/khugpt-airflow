import pinecone
import os

from dags.modules.utils import singleton
import openai
from dotenv import load_dotenv
from typing import Dict

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")


@singleton
class PineconeClient:
    
    def __init__(self, index_name: str, pinecone_api_key: str, environment: str, openai_api_key: str ):
        openai.api_key = openai_api_key
        pinecone.init(api_key=pinecone_api_key, environment=environment)
        self.pinecone_index = self._get_pinecone_index(index_name)

    def _transform_pinecone_vector(self, document: Dict):
        """
        Input document example: {_id: "", page_content: "", page_url: "", collected_at: ""}
        """
        return {
            "id": str(document["_id"]),
            "values": self._get_embedding(document["page_content"]),
            "metadata": {
                "page_url": document["page_url"], 
                "collected_at": document["collected_at"]
            }
        }

    def _get_embedding(self, text: str, model="text-embedding-ada-002"):
        text = text.replace("\n", " ")
        return openai.Embedding.create(input = [text], model=model)['data'][0]['embedding']

    def _get_pinecone_index(self, index_name: str, pool_threads: int = 4):
        indexes = pinecone.list_indexes()
        if index_name in indexes:
            index = pinecone.Index(index_name, pool_threads=pool_threads)
        elif len(indexes) == 0:
            raise ValueError("No active indexes found in your Pinecone project.")
        else:
            raise ValueError(
                f"Index '{index_name}' not found in your Pinecone project. "
            )
        return index

    def upsert_documents(self, documents):
        vectors = [self._transform_pinecone_vector(doc) for doc in documents]
        self.pinecone_index.upsert(vectors)




