from datetime import date, timedelta
from typing import Dict, List

from langchain.text_splitter import CharacterTextSplitter
from PyPDF2 import PdfReader
from tqdm import tqdm

from dags.modules.collect.base import BaseCollector
from dags.modules.database.pymongo import PymongoClient


class PdfCollector(BaseCollector):
    today_date = date.today()
    tomarrow_date = date.today() + timedelta(days=1)

    def __init__(self):
        self.documents = []

    def collect(
        self,
        pdf_path: str,
        reference_link: str = "https://ghaksa.khu.ac.kr/ghaksa/user/bbs/BMSR00048/list.do?menuNo=8500013",
    ) -> List[Dict]:
        self.reference_link = reference_link
        self.documents = self._get_documents(pdf_path)
        return self.documents

    def upload_db(self, db_host: str = "localhost", db_port: str = 27017) -> None:
        client = PymongoClient(host=db_host, port=db_port)
        client.insert_documents(self.documents)

    def _convert_to_json(self, chunks: List[Dict]) -> List[Dict]:
        items = []
        for chunk in chunks:
            items.append(
                {
                    "page_content": chunk,
                    "page_url": self.reference_link,
                    "collected_at": str(date.today()),
                }
            )

        return items

    def _get_documents(
        self, pdf_path: str, chunk_size: int = 2000, chunk_overlap: int = 200
    ) -> List[Dict]:
        text = ""
        pdf_reader = PdfReader(pdf_path)
        for page in tqdm(pdf_reader.pages):
            text += page.extract_text()
        text_splitter = CharacterTextSplitter(
            separator="\n",
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
        )
        chunks = text_splitter.split_text(text)
        json_documents = self._convert_to_json(chunks)
        return json_documents
