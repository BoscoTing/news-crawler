import asyncio

from scraper.config.config import settings
from scraper.core.factory import ScraperFactory
from scraper.core.processor import SitemapProcessor
from scraper.core.observer import LoggingObserver, StatisticsObserver
from scraper.core.scraper import Scraper


async def main():
    selectors = {
        'title': 'h1',
        'category': '.breadcrumb__item',
        'content': '#article_body',
        'keywords': '.article-keyword__item',
        'published_at': '.article-body__time'
    }
    s3_config = {
        "bucket_name": settings.S3_BUCKET_NAME,
        "base_path": settings.S3_BASE_PATH,
        "region_name": settings.S3_REGION_NAME,
    }
    
    # Make factory create components
    factory = ScraperFactory()
    downloader = factory.create_downloader()
    parser = factory.create_parser(selectors)
    storage = factory.create_storage(
        storage_type="s3-parquet",
        s3_config=s3_config,
    )    
    scraper = Scraper(
        downloader=downloader,
        parser=parser,
        storage=storage,
        max_concurrent=3
    )
    
    # Add observers
    logging_observer = LoggingObserver()
    stats_observer = StatisticsObserver()
    scraper.add_observer(logging_observer)
    scraper.add_observer(stats_observer)

    # Inject scraper to sitemap processor
    sitemap_processor = SitemapProcessor(
        scraper, 
        time_range=(settings.START_DATE, settings.END_DATE)
    )
    

    sitemap_url = settings.UDN_SITEMAP_URL
    results = await sitemap_processor.process_url(sitemap_url)
    
    # Check statistics
    print(f"Succeeded: {stats_observer.success_count}")
    print(f"Failed: {stats_observer.error_count}")


if __name__ == "__main__":
    asyncio.run(main())
