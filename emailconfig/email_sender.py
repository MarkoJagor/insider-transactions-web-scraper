import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from database.sql_queries import get_issuer_related_emails

logger = logging.getLogger(__name__)

EMAIL_ADDRESS = 'insidertehingud@gmail.com'
EMAIL_PASSWORD = os.environ.get('INSIDER_TRANSACTIONS_EMAIL_PW')
MAIN_LIST_ISSUERS = ["Arco", "Baltika", "Coop", "Ekspress", "EfTEN", "Enefit", "Harju", "Hepsor",
                     "LHV", "Merko", "Nordecon", "Pro Kapital", "PRFoods", "Silvano", "Tallink",
                     "Kaubamaja", "Sadam", "Vesi"]
SECONDARY_LIST_ISSUERS = ["Horizon", "Fibreboard", "Trigon"]
FIRST_NORTH_ISSUERS = ["Airobot", "Bercman", "Japan", "ELMO", "Hagen", "Linda", "TextMagic", "Modera", "Saunum"]


def send_email_to_subscribers(cursor, transaction_details):
    try:
        for issuer in MAIN_LIST_ISSUERS:
            if issuer in transaction_details[4]:
                set_email_settings(cursor, issuer, transaction_details)
                return

        for issuer in SECONDARY_LIST_ISSUERS:
            if issuer in transaction_details[4]:
                set_email_settings(cursor, issuer, transaction_details)
                return

        for issuer in FIRST_NORTH_ISSUERS:
            if issuer in transaction_details[4]:
                set_email_settings(cursor, issuer, transaction_details)
                return

        logger.info("Issuer was not found from specified issuer lists")

    except smtplib.SMTPException as err:
        logger.exception("Error occurred while trying to send email: " + repr(err))
    except Exception as err:
        logger.exception("Error occurred during scraping process: " + repr(err))


def set_email_settings(cursor, issuer, transaction_details):
    sql = get_issuer_related_emails()
    values = ['%' + issuer + '%']
    cursor.execute(sql, values)
    emails = [item[0] for item in cursor.fetchall()]

    if emails:
        smtp = smtplib.SMTP('smtp.gmail.com', 587)
        smtp.starttls()
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Ettevõtte %s juhtimiskohustusi täitva või lähedalt seotud isiku tehing" % transaction_details[4]
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = ', '.join(emails)

        msg.attach(MIMEText(create_email_content(transaction_details), 'html'))

        smtp.send_message(msg)
        smtp.quit()
        logger.info("Successfully sent emails to " + str(len(emails)) + " subscribers")
    else:
        logger.info("Found no emails to send subscription letters to.")


def create_email_content(transaction_details):
    trade_date = transaction_details[0]
    published = transaction_details[1]
    investor = transaction_details[2]
    investor_position = transaction_details[3]
    issuer = transaction_details[4]
    instrument = transaction_details[5]
    transaction_type = transaction_details[6]
    volume = transaction_details[7]
    price = transaction_details[8]
    market = transaction_details[9]
    has_been_updated = 'Algne teade' if transaction_details[10] == 0 else 'Muudatus'
    update_reason = transaction_details[11] if has_been_updated == 'Muudatus' else '-'
    total_price = round(float(volume) * float(price), 2)

    html = """\
        <html>
          <body>
            <h2>Börsiemitendiga seotud isiku tehingu detailid</h2>
            <h3>Emitent</h3>
            <p><b>Nimi: </b>{issuer}</p>
            <h3>Juhtimiskohustusi täitva isiku / temaga lähedalt seotud isiku andmed</h3>
            <p><b>Nimi: </b>{investor}</p>
            <p><b>Positsioon: </b>{investor_position}</p>
            <h3>Finantsinstrumendi ning tehingu tüüp</h3>
            <p><b>Finantsinstrumendi tüüp: </b>{instrument}</p>
            <p><b>Tehingu tüüp: </b>{transaction_type}</p>
            <h3>Tehingu maht ning väärtus</h3>
            <p><b>Tehingu maht: </b>{volume}</p>
            <p><b>Tehingu hind ühe ühiku kohta: </b>{price}</p>
            <p><b>Tehingu koondhind: </b>{total_price}</p>
            <h3>Muud andmed</h3>
            <p><b>Tehingu kuupäev: </b>{trade_date}</p>
            <p><b>Tehingu avalikustamise kuupäev: </b>{published}</p>
            <p><b>Tehingu koht: </b>{market}</p>
            <p><b>Algne teade / muudatus: </b>{has_been_updated}</p>
            <p><b>Muudatuse põhjus: </b>{update_reason}</p>
          </body>
        </html>
    """.format(issuer=issuer, investor=investor, investor_position=investor_position, instrument=instrument, transaction_type=transaction_type,
               volume=volume, price=price, total_price=total_price, trade_date=trade_date, published=published, market=market,
               has_been_updated=has_been_updated, update_reason=update_reason)

    return html
