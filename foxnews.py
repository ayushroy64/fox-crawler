import scrapy


class FoxnewsSpider(scrapy.Spider):
    name = "foxnews"
    allowed_domains = ["foxnews.com"]
    start_urls = ["https://foxnews.com"]

    def parse(self, response):
        pass
