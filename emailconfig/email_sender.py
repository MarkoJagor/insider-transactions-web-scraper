import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from database.sql_queries import get_issuer_related_emails, get_market_beater_emails

logger = logging.getLogger(__name__)

EMAIL_ADDRESS = 'insidertehingud@gmail.com'
EMAIL_PASSWORD = os.environ.get('INSIDER_TRANSACTIONS_EMAIL_PW')


def send_email_to_subscribers(cursor, transaction_details, issuer, is_potential_market_beater, days_to_published):
    try:
        set_email_settings(cursor, issuer, transaction_details, is_potential_market_beater, days_to_published)
    except smtplib.SMTPException as err:
        logger.exception("Error occurred while trying to send email: " + repr(err))
    except Exception as err:
        logger.exception("Error occurred during scraping process: " + repr(err))


def set_email_settings(cursor, issuer, transaction_details, is_potential_market_beater, days_to_published):
    # Get emails that have subscribed to issuer
    sql = get_issuer_related_emails()
    values = [issuer]
    cursor.execute(sql, values)
    emails = [item[0] for item in cursor.fetchall()]

    # Get emails that have subscribed to potential market beating transactions
    if is_potential_market_beater:
        sql = get_market_beater_emails()
        cursor.execute(sql)
        market_beater_emails = [item[0] for item in cursor.fetchall()]

        # Merge email lists and remove potential duplicates
        emails = list(set(emails + market_beater_emails))

    if emails:
        # Configure email settings
        smtp = smtplib.SMTP('smtp.gmail.com', 587)
        smtp.starttls()
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Ettevõtte %s juhtimiskohustusi täitva või lähedalt seotud isiku tehing" % transaction_details[4]
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = ', '.join(emails)

        # Attach html content to email
        email_content = create_email_content(transaction_details, is_potential_market_beater, days_to_published)
        msg.attach(MIMEText(email_content, 'html'))

        smtp.send_message(msg)
        smtp.quit()
        logger.info("Successfully sent emails to " + str(len(emails)) + " subscribers")
    else:
        logger.info("Found no emails to send subscription letters to.")


def create_email_content(transaction_details, is_potential_market_beater, days_to_published):
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
    """.format(issuer=issuer, investor=investor, investor_position=investor_position, instrument=instrument, transaction_type=transaction_type,
               volume=volume, price=price, total_price=total_price, trade_date=trade_date, published=published, market=market,
               has_been_updated=has_been_updated, update_reason=update_reason)

    html_ending = "</body></html>"

    if is_potential_market_beater:
        short_term_returns, long_term_returns = get_returns(days_to_published)
        html_market_beater = """\
        <h3>Tallinna börsiindeksi ületamise potentsiaal</h3>
        <p><b>Tehingu liik: </b>{transaction_type}</p>
        <p><b>Tehingu tähtaeg: </b>{published}</p>
        <p><b>Väärtpaberi hoidmise aeg: </b>21 kauplemispäeva</p>
        <p><b>Ajaloolised andmed: </b>Tallinna börsi ületav tootlus vahemikus 21.11.18-28.02.22: <b>{short_term_returns}%</b>. 
        Tallinna börsi ületav tootlus vahemikus 01.04.05-28.02.22: <b>{long_term_returns}%</b></p>
        """.format(transaction_type=transaction_type, published=published, short_term_returns=short_term_returns, long_term_returns=long_term_returns)

        return html + html_market_beater + html_ending

    return html + html_ending


def get_returns(days_to_published):
    if days_to_published == 0:
        return 1.86, 1.60
    elif days_to_published == 1:
        return 1.16, 1.30
    elif days_to_published == 2:
        return 1.16, 1.24
    elif days_to_published == 3:
        return 1.16, 1.38
    elif days_to_published == 4:
        return 1.16, 1.13
