from datetime import date, datetime, timedelta
from typing import Dict, List
from urllib.parse import urljoin

import requests
from tqdm import tqdm

from dags.modules.collect.base import BaseCollector
from dags.modules.database.pymongo import PymongoClient


class KhutodayCollector(BaseCollector):
    today_date = date.today()
    tomarrow_date = date.today() + timedelta(days=1)

    def __init__(self):
        self.links = []
        self.documents = []

    def collect(
        self,
        start_date: datetime.date = today_date,
        end_date: datetime.date = tomarrow_date,
    ) -> List[Dict]:
        self.links = self._get_links(start_date, end_date)
        self.documents = self._get_documents(self.links)
        return self.documents

    def upload_db(self, db_host: str = "localhost", db_port: int = 27017) -> None:
        client = PymongoClient(host=db_host, port=db_port)
        client.insert_documents(self.documents)

    def _get_documents(self, links: List[str]) -> List[Dict]:
        documents = []
        for link in tqdm(links):
            response = requests.get(link)
            if "STACKS" not in response.json().keys():
                print(f"{link} is not valid.")
                continue
            stacks = response.json()["STACKS"]
            for stack in stacks:
                # If page content is empty.
                if not stack[2]:
                    continue
                item = {
                    "page_content": stack[2],
                    "page_url": stack[3],
                    "collected_at": stack[1],
                }
                documents.append(item)
        return documents

    def _get_links(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        host: str = "http://163.180.142.196:9090/today_api/",
    ) -> List[str]:
        links = []
        delta = end_date - start_date
        for i in range(delta.days):
            day = start_date + timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            link = urljoin(base=host, url=day_str)
            links.append(link)
        return links
