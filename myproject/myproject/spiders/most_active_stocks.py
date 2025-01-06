import scrapy
from elasticsearch import Elasticsearch
from datetime import datetime

class MostActiveStocksHTMLSpider(scrapy.Spider):
    name = "most_active_stocks_html"
    allowed_domains = ["live.euronext.com"]
    start_urls = [
        "https://live.euronext.com/en/ajax/getTopPerformersPopup/MostActive?a=true&belongs_to=ENXL,ALXL,XLIS&is_factory=true&tp_type=STOCK&tp_subtype=0101"
    ]

    # Elasticsearch connection details
    ELASTICSEARCH_HOST = ""
    ELASTICSEARCH_USER = "root"
    ELASTICSEARCH_PASSWORD = ""
    INDEX_NAME = "psi20lisbon"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize Elasticsearch client
        self.es = Elasticsearch(
            self.ELASTICSEARCH_HOST,
            http_auth=(self.ELASTICSEARCH_USER, self.ELASTICSEARCH_PASSWORD)
        )

    def parse(self, response):
        # Locate the table
        table = response.xpath("//table[@id='AwlTopPerformersPopupTable']")

        # Extract column headers
        headers = table.xpath(".//thead/tr/th/text()").getall()
        headers = [header.strip() for header in headers if header.strip()]  # Clean and remove empty headers

        # Extract rows from tbody
        rows = table.xpath(".//tbody/tr")

        for row in rows:
            # Extract each cell's text and align with headers
            cells = row.xpath("./td")
            data = {}
            for index, cell in enumerate(cells):
                if index < len(headers):
                    column_name = headers[index]
                    cell_text = cell.xpath(".//text()").get(default="").strip()
                    cell_data = cell.attrib.get("data-order", cell_text)  # Prefer 'data-order' if available
                    data[column_name] = cell_data

            # Add timestamp to the data
            data['timestamp'] = datetime.utcnow().isoformat()

            # Index the data into Elasticsearch
            self.index_to_elasticsearch(data)

        # Handle additional tables if needed
        other_table = response.xpath("//table[@id='AwlTopPerformersPopupTableDownload']")
        if other_table:
            yield from self.parse_table(other_table)

    def parse_table(self, table):
        # Extract headers
        headers = table.xpath(".//thead/tr/th/text()").getall()
        headers = [header.strip() for header in headers if header.strip()]

        # Extract rows
        rows = table.xpath(".//tbody/tr")

        for row in rows:
            cells = row.xpath("./td")
            data = {}
            for index, cell in enumerate(cells):
                if index < len(headers):
                    column_name = headers[index]
                    cell_text = cell.xpath(".//text()").get(default="").strip()
                    cell_data = cell.attrib.get("data-order", cell_text)
                    data[column_name] = cell_data

            # Add timestamp to the data
            data['timestamp'] = datetime.utcnow().isoformat()

            # Index the data into Elasticsearch
            self.index_to_elasticsearch(data)

    def index_to_elasticsearch(self, data):
        try:
            # Index the document into Elasticsearch
            self.es.index(index=self.INDEX_NAME, document=data)
            self.logger.info(f"Indexed data: {data}")
        except Exception as e:
            self.logger.error(f"Failed to index data: {data}, Error: {e}")
