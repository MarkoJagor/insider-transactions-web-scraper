import logging
import sys

import MySQLdb
from MySQLdb import Error

logger = logging.getLogger(__name__)

# Database connection properties
HOST = "localhost"
USERNAME = "root"
PASSWORD = ""
DATABASE = "transactions"


def create_server_connection():
    try:
        database = MySQLdb.connect(HOST, USERNAME, PASSWORD, DATABASE)
        logger.info("MySQL Database connection successful")
    except Error as err:
        logger.exception(f"Failed to connect to MySQL Database: '{err}'")
        sys.exit(1)

    return database
