import pinecone
import os

from dags.modules.utils import singleton
import openai
from dotenv import load_dotenv
from typing import Dict, List, Iterable
from tqdm import tqdm
from itertools import islice

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def batch_iterate(size: int, iterable):
    """Utility batching function."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


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

    def _get_embeddings(self, texts: List[str], model="text-embedding-ada-002"):
        texts = [text.replace("\n", " ") for text in texts]
        responses = openai.Embedding.create(input = texts, model=model)['data']
        embeddings = [res['embedding'] for res in responses]
        return embeddings

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

    def upsert_documents(self, documents, batch_size: int = 32, embedding_chunk_size: int = 1000):
        contents = []
        ids = []
        metadatas = []
        for doc in documents:
            contents.append(doc['page_content'])
            ids.append(str(doc['_id']))
            metadatas.append({"page_url": doc["page_url"], 
                "collected_at": doc["collected_at"]})
            
        for i in range(0, len(documents), embedding_chunk_size):
            chunk_contents = contents[i:i+embedding_chunk_size]
            chunk_ids = ids[i:i+embedding_chunk_size]
            chunk_metadatas = metadatas[i:i+embedding_chunk_size]
            embeddings = self._get_embeddings(chunk_contents)
            async_res = [
                self.pinecone_index.upsert(batch, async_req=True)
                for batch in batch_iterate(batch_size, zip(chunk_ids, embeddings, chunk_metadatas))
            ]
            [res.get() for res in async_res]




