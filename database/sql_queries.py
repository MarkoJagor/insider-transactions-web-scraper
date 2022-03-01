def get_transaction():
    return "SELECT * FROM transactions.transaction " \
           "WHERE trade_date = %s " \
           "AND published_date = %s AND issuer = %s " \
           "AND investor = %s AND volume = %s " \
           "AND price = %s;"


def insert_new_transaction():
    return "INSERT INTO transactions.transaction (trade_date, published_date, investor, investor_position, issuer, instrument," \
           " transaction_type, volume, price, market, has_been_updated, update_reason)" \
           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


def get_most_recent_published_date():
    return "SELECT MAX(transactions.transaction.published_date) FROM transactions.transaction"


def get_issuer_related_emails():
    return "SELECT transactions.account.username FROM transactions.account " \
           "WHERE transactions.account.account_id IN (SELECT transactions.account_issuer.account_id FROM account_issuer " \
           "WHERE transactions.account_issuer.issuer_id = (SELECT transactions.issuer.issuer_id FROM transactions.issuer " \
           "WHERE transactions.issuer.name = %s))"


def get_issuer_names_shortened():
    return "SELECT transactions.issuer_shortened.name_shortened FROM transactions.issuer_shortened"


def get_issuer_name_like_issuer_name_shortened():
    return "SELECT transactions.issuer.name FROM transactions.issuer " \
           "WHERE transactions.issuer.name LIKE %s"
