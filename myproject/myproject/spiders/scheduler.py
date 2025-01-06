from apscheduler.schedulers.blocking import BlockingScheduler
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from .most_active_stocks import MostActiveStocksHTMLSpider

def run_spider():
    print("Starting the spider...")
    process = CrawlerProcess(get_project_settings())
    process.crawl(MostActiveStocksHTMLSpider)
    process.start()

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    # Schedule the spider to run every hour
    scheduler.add_job(run_spider, "interval", hours=1)
    print("Scheduler is running. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")
