from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Dict, List

import html2text
from bs4 import BeautifulSoup
from tqdm import tqdm

from dags.modules.utils.image_ocr import get_image_ocr


class BaseCollector(ABC):
    """Abstract base class for Collector."""

    today_date = date.today()
    tomarrow_date = date.today() + timedelta(days=1)

    def __init__(self, target_link: str):
        self.links = []
        self.documents = []
        self.target_link = target_link

    @abstractmethod
    def collect(
        self, start_date: datetime.date, end_date: datetime.date, max_page: int = 35
    ) -> List[Dict]:
        """Perform collecting documents for a specified period.

        Args:
            start_date: The start date of the article to begin collecting.
            end_date: The end date of the article to finish collecting.
            max_page: The maximum number of pages of links to collect from the target of the collector.

        Returns:
            A list of documents, each containing page content.

        """

    @abstractmethod
    def upload_db(self, db_host: str = "localhost", db_port: int = 27017) -> None:
        """Uploads documents to the database at the given address.

        Args:
            db_host: Address of the host DB.
            db_port: Port number of the host DB.

        Return:
            None

        """

    @abstractmethod
    def _get_links(
        self, start_date: datetime.date, end_date: datetime.date, max_page: int = 35
    ) -> List[str]:
        """Get links from the target link of the collector over a specified period.

        Args:
            start_date: The start date of the article to begin collecting.
            end_date: The end date of the article to finish collecting.
            max_page: The maximum number of pages of links to collect from the target of the collector.

        Returns:
            A list of links (str).

        """

    @abstractmethod
    def _get_documents(self, links: str) -> List[Dict]:
        """Retrieve the content within the post from the provided links.

        Args:
            links: A list of links (str).

        Returns:
            A list of documents, each containing page content.

        """

    def _transform_documents(
        self,
        documents: List[Dict],
        scope_selector: str = ".content-box",
        get_image: bool = True,
        ignore_links: bool = False,
        ignore_images: bool = True,
    ) -> List[Dict]:
        """Transform the provided documents into meaningful content.

        This method performs OCR analysis on the provided document (optional),
        removing unnecessary sections and converting images to text.

        Args:
            documents: A list of documents.
            scope_selector: The selector of the body of the page to be extracted.
            get_image: Whether images should be converted to text using OCR; defaults to True.
            ignore_links: Whether links should be ignored; defaults to False.
            ignore_images: Whether images should be ignored; defaults to True.

        Returns:
            A list of documents, each containing page content.

        """
        h = html2text.HTML2Text()
        h.ignore_links = ignore_links
        h.ignore_images = ignore_images

        for document in tqdm(documents):
            page_text = ""
            img_text = ""
            soup = BeautifulSoup(document.page_content, "html.parser")
            scope_html = str(soup.select_one(scope_selector))
            if get_image:
                img_links = self._extract_img_links(scope_html)
                print(img_links)
                for img_link in img_links:
                    img_text += f" {get_image_ocr(img_link)}"
            page_text = h.handle(scope_html)
            document.page_content = page_text + img_text
        return documents

    def _extract_img_links(self, html: str) -> List[str]:
        """Extracts the paths of the image tags from a HTML formatted string.

        Args:
            html: a HTML formatted string.

        Returns:
            A list of links (str).
        """
        soup = BeautifulSoup(html, "html.parser")
        img_tags = soup.find_all("img")
        links = []
        for img_tag in img_tags:
            src = img_tag["src"]
            if (".jpg" in src) or (".png" in src):
                links.append(img_tag["src"])
        return links

    def _convert_to_json(self, documents: List[Dict]) -> List[Dict]:
        """Converts documents to a suitable form for DB loading.

        Args:
            documents: A list of documents.

        Returns:
            A list of coverted docuements.

        """
        items = []
        for document in documents:
            items.append(
                {
                    "page_content": document.page_content,
                    "page_url": document.metadata["source"],
                    "collected_at": str(date.today()),
                }
            )

        return items
