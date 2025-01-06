import scrapy


class ExampleSpider(scrapy.Spider):
    name = "example"
    allowed_domains = ["live.euronext.com"]
    start_urls = ["https://live.euronext.com/en/markets/lisbon"]

    def parse(self, response):
        pass