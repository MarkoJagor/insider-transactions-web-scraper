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
