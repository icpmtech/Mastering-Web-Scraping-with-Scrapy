import scrapy
import json

class EuronextTopPerformersSpider(scrapy.Spider):
    name = "euronext_top_performers"
    allowed_domains = ["live.euronext.com"]
    start_urls = ["https://live.euronext.com/en/popout-page/getTopPerformers"]

    def parse(self, response):
        # Parse the JSON response
        try:
            data = json.loads(response.text)
            for performer in data.get("rows", []):  # Adjust key based on the actual API response
                yield {
                    "name": performer.get("name"),
                    "symbol": performer.get("symbol"),
                    "price": performer.get("price"),
                    "change": performer.get("change"),
                    "percentage_change": performer.get("percentageChange"),
                    "volume": performer.get("volume"),
                }
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")
