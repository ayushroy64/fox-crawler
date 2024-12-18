import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import csv

class FoxNewsSpider(CrawlSpider):
    name = "foxnews"
    allowed_domains = ["foxnews.com"]
    start_urls = ["https://www.foxnews.com"]

    rules = (
        Rule(LinkExtractor(allow=r'https://www.foxnews.com/.*'), callback='parse_item', follow=True),
    )

    custom_settings = {
        'DEPTH_LIMIT': 16,
        'CLOSESPIDER_PAGECOUNT': 20000,
    }

    fetch_fox = 'fetch_FoxNews2.csv'
    visit_fox = 'visit_FoxNews2.csv'
    urls_fox = 'urls_FoxNews2.csv'
    report_file = 'CrawlReport_FoxNews2.txt'

    page_count = 0
    success_count = 0
    failed_count = 0
    urls_extracted = set()
    file_sizes = {
        "< 1KB": 0,
        "1KB ~ <10KB": 0,
        "10KB ~ <100KB": 0,
        "100KB ~ <1MB": 0,
        ">= 1MB": 0
    }
    content_types = {}

    def __init__(self, *args, **kwargs):
        super(FoxNewsSpider, self).__init__(*args, **kwargs)

        self.fetch_fox_csv = open(self.fetch_fox, mode='w', newline='', encoding='utf-8')
        self.visit_fox_csv = open(self.visit_fox, mode='w', newline='', encoding='utf-8')
        self.urls_fox_csv = open(self.urls_fox, mode='w', newline='', encoding='utf-8')

        self.fetch_fox_writer = csv.writer(self.fetch_fox_csv)
        self.visit_fox_writer = csv.writer(self.visit_fox_csv)
        self.urls_fox_writer = csv.writer(self.urls_fox_csv)

        self.fetch_fox_writer.writerow(['URL', 'Status'])
        self.visit_fox_writer.writerow(['URL', 'Size (Bytes)', 'Outlinks', 'Content-Type'])
        self.urls_fox_writer.writerow(['URL', 'Indicator'])

    def parse_item(self, response):
        self.page_count += 1
        url = response.url
        status = response.status
        content_size = len(response.body)
        type = response.headers.get('Content-Type', b'').decode('utf-8')
        out_links = LinkExtractor().extract_links(response)

        self.file_size_counter(content_size)

        if self.page_count <= 20000:
            self.fetch_fox_writer.writerow([url, status])

        self.visit_fox_writer.writerow([url, content_size, len(out_links), type])

        if type not in self.content_types:
            self.content_types[type] = 0
        self.content_types[type] += 1

        for link in out_links:
            indicator = "OK" if link.url.startswith("https://www.foxnews.com") else "N_OK"
            self.urls_fox_writer.writerow([link.url, indicator])
            self.urls_extracted.add(link.url)

        if status == 200:
            self.success_count += 1
        else:
            self.failed_count += 1

        if self.page_count >= 20000:
            self.crawler.engine.close_spider(self, "Max Limit of Pages")

    def file_size_counter(self, size):
        if size < 1024:
            self.file_sizes["< 1KB"] += 1
        elif size < 10240:
            self.file_sizes["1KB ~ <10KB"] += 1
        elif size < 102400:
            self.file_sizes["10KB ~ <100KB"] += 1
        elif size < 1048576:
            self.file_sizes["100KB ~ <1MB"] += 1
        else:
            self.file_sizes[">= 1MB"] += 1

    def urls_counter(self):
        total_urls = 0
        unique_urls = set()
        foxnews_urls = 0
        non_foxnews_urls = 0

        with open(self.urls_fox, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                if len(row) != 2:
                    continue

                url, indicator = row
                total_urls += 1
                unique_urls.add(url)

                if indicator == 'OK':
                    foxnews_urls += 1
                else:
                    non_foxnews_urls += 1

        return total_urls, len(unique_urls), foxnews_urls, non_foxnews_urls

    def closed(self, reason):
        self.fetch_fox_csv.close()
        self.visit_fox_csv.close()
        self.urls_fox_csv.close()
        self.generate_report()

    def generate_report(self):
        total_urls, unique_urls_count, foxnews_urls, non_foxnews_urls = self.urls_counter()

        with open(self.report_file, 'w') as report:
            report.write(f"Name: Ayush Roy\n")
            report.write(f"USC ID: 2668011543\n")
            report.write(f"News site crawled: foxnews.com\n")
            report.write(f"Number of threads: 16\n")
            report.write(f"\nFetch Statistics\n================\n")
            report.write(f"# fetches attempted: {self.page_count}\n")
            report.write(f"# fetches succeeded: {self.success_count}\n")
            report.write(f"# fetches failed or aborted: {self.failed_count}\n")
            report.write(f"\nOutgoing URLs:\n==============\n")
            report.write(f"Total URLs extracted: {total_urls}\n")
            report.write(f"# unique URLs extracted: {unique_urls_count}\n")
            report.write(f"# unique URLs within News Site: {foxnews_urls}\n")
            report.write(f"# unique URLs outside News Site: {non_foxnews_urls}\n")
            report.write(f"\nStatus Codes:\n=============\n")
            report.write(f"200 OK: {self.success_count}\n")
            report.write(f"404 Not Found: {self.failed_count}\n")
            report.write(f"\nFile Sizes:\n===========\n")
            for size_category, count in self.file_sizes.items():
                report.write(f"{size_category}: {count}\n")

            report.write(f"\nContent Types:\n==============\n")
            for content_type, count in self.content_types.items():
                report.write(f"{content_type}: {count}\n")

if __name__ == "__main__":
    process = CrawlerProcess(settings={
        'LOG_LEVEL': 'INFO',
        'AUTOTHROTTLE_ENABLED': True,
        'CONCURRENT_REQUESTS': 16,      # Parallelization for fast execution
    })
    
    process.crawl(FoxNewsSpider)
    process.start()
