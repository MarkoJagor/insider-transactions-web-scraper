import logging
from datetime import datetime as dt

import requests
from MySQLdb import Error
from bs4 import BeautifulSoup

from database.sql_queries import get_most_recent_published_date, get_transaction, insert_new_transaction
from emailconfig.email_sender import send_email_to_subscribers

logger = logging.getLogger(__name__)

# URLs to be scraped
transactionsUrl = 'http://oam.fi.ee/et/transaction-list'
baseUrl = 'http://oam.fi.ee'


def get_sorted_table_data_by_avalikustatud(transactions_table):
    url_path_sort_by_avalikustatud = (transactions_table.find("a", string="Avalikustatud").get("href"))[2:]
    html = requests.get(baseUrl + url_path_sort_by_avalikustatud)
    soup = BeautifulSoup(html.text, "html.parser")
    return soup.find("table", {'class': 'searchContent'})


def scrape_transaction_data(cursor, db):
    try:
        # Load html's plain data into a variable and parse the data
        html = requests.get(transactionsUrl)
        soup = BeautifulSoup(html.text, "html.parser")

        # Get initial transactions table
        transactions_table_initial = soup.find("table", {'class': 'searchContent'})

        # Sort transactions table twice to get table data from newest to oldest
        transactions_table_oldest_first = get_sorted_table_data_by_avalikustatud(transactions_table_initial)
        transactions_table_newest_first = get_sorted_table_data_by_avalikustatud(transactions_table_oldest_first)

        # Get most recently published transaction date to compare against transactions on page
        sql = get_most_recent_published_date()
        cursor.execute(sql)
        max_published_date = cursor.fetchone()[0]

        new_transactions = []

        for row in reversed(transactions_table_newest_first.find_all("tr")[1:]):
            cells = row.find_all("td")
            published_string = cells[5].text.strip()[0:10]
            published_date = dt.strptime(published_string, "%Y-%m-%d").date()

            if published_date < max_published_date:
                continue

            trade_date_string = cells[0].text.strip()[0:10]
            issuer_string = cells[1].text.strip()
            investor_string = cells[2].text.strip()
            volume_string = cells[3].text.strip()
            price_string = cells[4].text.strip()

            values = (trade_date_string, published_string, issuer_string, investor_string, volume_string, price_string)
            sql = get_transaction()
            cursor.execute(sql, values)
            transaction = cursor.fetchall()

            if not transaction:
                transaction_details = scrape_transaction_details(cells)
                values = (trade_date_string, published_string, investor_string, transaction_details["investor_position"], issuer_string,
                          transaction_details["instrument"], transaction_details["transaction_type"], volume_string, price_string,
                          transaction_details["market"], transaction_details["has_been_updated"], transaction_details["update_reason"])

                sql = insert_new_transaction()
                cursor.execute(sql, values)
                db.commit()

                send_email_to_subscribers(cursor, values)

                new_transactions.append(values)
                logger.info("Inserted new transaction - trade date: %s, published date: %s, investor: %s, investor_position: %s, issuer: %s,"
                            "instrument: %s, transaction type: %s, volume: %s, price: %s, market: %s, has been updated: %s, update reason: %s"
                            % values)

        if not new_transactions:
            logger.info("No transactions were added during process")
    except Error as err:
        logger.exception("Error occurred while executing database operation: " + repr(err))
    except (AttributeError, IndexError) as err:
        logger.exception("Error occurred during scraping process: " + repr(err))


def scrape_transaction_details(cells):
    url_path_transaction_details = (cells[0].find("a").get("href"))[2:]
    html = requests.get(baseUrl + url_path_transaction_details)
    soup = BeautifulSoup(html.text, "html.parser")

    transaction_details_table = soup.find("table", {'class': 'tableField'})
    transaction_details_dictionary = {
        "investor_position": "",
        "instrument": "",
        "transaction_type": "",
        "market": "",
        "has_been_updated": False,
        "update_reason": "",
    }
    for row in transaction_details_table.find_all("tr")[3:]:
        cells = row.find_all("td")

        if len(cells) > 1:
            header_cell = cells[0].text.strip()
            content_cell = cells[1].text.strip()
        else:
            continue

        if header_cell == "Positsioon/staatus:":
            transaction_details_dictionary.update({"investor_position": content_cell})
        elif header_cell == "Algne teade / muudatus:":
            if content_cell == "Algne teade":
                continue
            else:
                transaction_details_dictionary.update({"has_been_updated": True})
        elif header_cell == "Muudatuse põhjus:":
            transaction_details_dictionary.update({"update_reason": content_cell})
        elif header_cell == "Finantsinstrumendi liik:":
            transaction_details_dictionary.update({"instrument": content_cell})
        elif header_cell == "Tehingu liik:":
            transaction_details_dictionary.update({"transaction_type": content_cell})
        elif header_cell == "Tehingu koht:":
            transaction_details_dictionary.update({"market": content_cell})
            break

    return transaction_details_dictionary
