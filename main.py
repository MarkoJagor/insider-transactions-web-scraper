import logging
import time

from database.db_connection import create_server_connection
from scraper.daily_scraper import scrape_transaction_data


def main():
    timestr = time.strftime("%Y-%m-%d")
    logging.basicConfig(filename="../ScraperLogs/scraper-info-" + timestr + ".log", level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    logger.info("Starting scraping process")
    db = create_server_connection()
    cursor = db.cursor()
    scrape_transaction_data(cursor, db)
    db.close()
    logger.info("Ending scraping process\n")


if __name__ == "__main__":
    main()
