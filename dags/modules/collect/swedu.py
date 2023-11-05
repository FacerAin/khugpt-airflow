from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from langchain.document_transformers import Html2TextTransformer
from langchain.document_loaders import AsyncHtmlLoader

from datetime import datetime, date, timedelta
from dags.modules.database.pymongo import PymongoClient

class SweduCollector:
    today_date = date.today()
    tomarrow_date = date.today() + timedelta(days=1)
    def __init__(self):
        self.links = []
        self.documents = []

    def collect(self, start_date:datetime.date =today_date , end_date:datetime.date=tomarrow_date):
        #TODO: Serve progress informations.
        self.links = self._get_links(start_date, end_date)
        self.documents = self._get_documents(self.links)
        return self.documents

    def upload_db(self):
        client = PymongoClient(host="localhost", port=27017)
        client.insert_documents(self.documents)


    def _convert_to_json(self, documents):
        items = []
        for document in documents:
            items.append({"page_content": document.page_content,"page_url": document.metadata["source"], "collected_at": str(date.today())})

        return items
    
    def _check_datetime_range(self, input_date: datetime.date, start_date: datetime.date, end_date: datetime.date):
        return start_date <= input_date <= end_date




    def _get_links(self, start_date, end_date, max_page:int = 34):
        #TODO: How to determine max page?
        links = []
        is_break = False
        for page in tqdm(range(1, max_page)):
            if is_break:
                break
            response = requests.get(f"https://swedu.khu.ac.kr/board5/bbs/board.php?bo_table=06_01&page={page}")
            soup = BeautifulSoup(response.text, 'html.parser')
            for item in soup.select('#fboardlist > div > table > tbody > tr'):
                link = item.find('a').get('href')
                content_date = datetime.strptime(item.select('.td_datetime')[0].getText(), "%Y-%m-%d").date()
                if not  self._check_datetime_range(content_date, start_date, end_date):
                    is_break = True
                    break
                links.append(link)
        return links
    
    def _get_documents(self, links):
        loader = AsyncHtmlLoader(links)
        html2text = Html2TextTransformer()
        documents = loader.load()
        transform_documents = html2text.transform_documents(documents)
        json_documents = self._convert_to_json(transform_documents)
        return json_documents








