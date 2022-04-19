import logging

import MySQLdb
import requests
from MySQLdb import Error
from bs4 import BeautifulSoup

logging.basicConfig(filename="../demo.log", level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s')
log = logging.getLogger("Scraper-logger")
# Database connection properties
HOST = "localhost"
USERNAME = "root"
PASSWORD = ""
DATABASE = "transactions"
# URL to be scraped
transactionsUrl = 'http://oam.fi.ee/et/transaction-list'
baseUrl = 'http://oam.fi.ee'


def create_server_connection(host_name, user_name, user_password, database_name):
    database = None
    try:
        database = MySQLdb.connect(host_name, user_name, user_password, database_name)
        log.info("MySQL Database connection successful")
    except Error as err:
        log.exception(f"Failed to connect to MySQL Database: '{err}'")

    return database


def scrape_transaction_data():
    # Load html's plain data into a variable and parse the data
    plain_html = requests.get(transactionsUrl)
    soup = BeautifulSoup(plain_html.text, "html.parser")

    # get url for page navigation
    pagination_list = soup.find("ul", {'class': 'pagination'})
    url_path_next_page = (pagination_list.find("a", string=">").get('href'))[2:]

    # set looping variables
    j = True
    i = 1

    while j:
        # Get table data
        transactions_table = soup.find("table", {'class': 'searchContent'})
        # Get table cells
        for count, row in enumerate(transactions_table.find_all("tr")[1:]):
            cells = row.find_all("td")
            trade_date_string = cells[0].text.strip()[0:10]
            issuer_string = cells[1].text.strip()
            investor_string = cells[2].text.strip()
            volume_string = cells[3].text.strip()
            price_string = cells[4].text.strip()
            published_string = cells[5].text.strip()[0:10]
            published_string_with_time = cells[5].text.strip()

            # Break loop on specific transaction
            if trade_date_string == '2018-11-13' and published_string == '2018-11-13' and issuer_string == 'AS Tallink Grupp':
                j = False
                break

            log.info("Index: " + str(i) + "; Date: " + trade_date_string + "; Issuer: " + issuer_string + "; Investor: " + investor_string + "; Volume: "
                     + volume_string + "; Price: " + price_string + "; Published: " + published_string)
            i += 1

            values = (published_string_with_time, trade_date_string, issuer_string, investor_string, volume_string, price_string)
            sql = update_transactions_table_data()
            cursor.execute(sql, values)

            if count == 19:
                log.info("Item with index " + str(i - 1) + " was last in page")
                html = requests.get(baseUrl + url_path_next_page)
                soup = BeautifulSoup(html.text, "html.parser")

                pagination_list = soup.find("ul", {'class': 'pagination'})
                url_path_next_page = (pagination_list.find("a", string=">").get('href'))[2:]


def update_transactions_table_data():
    return "UPDATE transactions.transaction_published " \
           "SET published = %s " \
           "WHERE trade_date = %s AND issuer = %s " \
           "AND investor = %s AND volume = %s " \
           "AND price = %s;"


db = create_server_connection(HOST, USERNAME, PASSWORD, DATABASE)
cursor = db.cursor()
scrape_transaction_data()
db.commit()
db.close()
