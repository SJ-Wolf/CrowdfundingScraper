#!/usr/bin/python

# app ID = edu.berkeley.haas.crowdfunding.kiva
from kiva import kiva_api
import sys
import db_connections
import logging
import useful_functions
import json
import traceback

exit()


def get_most_recent_loan_in_database(db, table='loan'):
    """

    :param db: database connection to use
    :param table: table to look at in the default schema of the database
    :return: id number of the most recent loan (by posted date)
    """
    r = db.query('select id from {} order by posted_date desc limit 1'.format(table))
    return r.next()['id']


def get_loans_to_scrape(kiva_db, api, loan_table='loan', per_page=500, from_file=None):
    """

    :param kiva_db: database kiva information is stored
    :param api: api to use (such as KivaAPI in kiva_api)
    :param loan_table: table loans are stored in
    :param per_page: how many loans to load per request
    :param from_file: if provided, loan to be scraped will be taken from the file, which should be a json formatted
    file that is a list of loan dictionaries
    :return: list of dictionaries describing the loans that are not in the database, plus a few extra
    """
    if from_file is None:
        most_recent_loan_in_database = get_most_recent_loan_in_database(db=kiva_db, table='loan')
        loans_to_scrape = api.get_most_recent_loans(stop_loan_id=most_recent_loan_in_database, per_page=per_page)
    else:
        with open(from_file) as f:
            loans_to_scrape = json.load(f)
    return loans_to_scrape


def get_lenders_to_scrape(kiva_db, lender_table='lender', loan_lender_table='loan_lender'):
    """

    :param kiva_db: database kiva information is stored
    :param lender_table: table lenders are stored in
    :param loan_lender_table: table that each loan's lenders are stored in
    :return: a list of lender ids
    """
    q = '''
        SELECT
            {1}.lender_id, {0}.lender_id as null_id
        FROM
            {1}
                LEFT JOIN
            {0} ON {0}.lender_id = {1}.lender_id
        #where not ({1}.lender_id > 0)
        group by {1}.lender_id
        having null_id is null;'''.format(lender_table, loan_lender_table)
    r = kiva_db.query(q)
    return [x['lender_id'] for x in r]


def upload_missing_lenders(kiva_db, api):
    """
    Uploads any lenders that are missing from the database

    :param kiva_db: database kiva information is stored
    :param api: api to use (such as KivaAPI in kiva_api)
    :return: None
    """
    lenders_to_scrape = get_lenders_to_scrape(kiva_db=kiva_db)
    all_lender_data = []
    for lender_ids_chunk in useful_functions.split_array_into_chunks(data=lenders_to_scrape, chunk_size=50):
        all_lender_data += api.get_lender_data(lender_ids_chunk)['lender']
    db_connections.uploadOutputFile(data=all_lender_data, db=kiva_db, table='lender', ensure=True)


def upload_new_loans_and_loan_lenders(db, api, from_file=None, update=True):
    """
    Uploads new loans and loan lenders to the database

    :param db: database to put new loan and loan_lender information in
    :param api: api to use (such as KivaAPI in kiva_api)
    :param from_file: uploads data from this file instead of querying the database to find what's missing
    :param update: whether to update database or insert missing data
    :return: None
    """
    newest_loans_json = get_loans_to_scrape(api=api, kiva_db=db, per_page=100, from_file=from_file)

    all_data = None
    for loan_data_chunk in useful_functions.split_array_into_chunks(newest_loans_json, 100):
        loan_data = api.get_detailed_loan_data(loan_data_chunk)
        loans_lenders_data = api.get_loans_lenders_data(loan_data_chunk)
        total_data = dict(loan_data.items() + loans_lenders_data.items())
        if all_data is None:
            all_data = total_data
        else:
            for key in all_data:
                all_data[key] += total_data[key]

    db_connections.multi_table_upload(all_data, db=db, update=update, ensure=True, ordered_tables=['loan'])


def update_current_projects(db, api):
    """
    Updates loans in the database that are still ongoing

    :param db: database to put new loan and loan_lender information in
    :param api: api to use (such as KivaAPI in kiva_api)
    :return: None
    """
    r = db.query("select * from loan where `status` in ('in_repayment', 'inactive', 'fundraising') and DATEDIFF(now(), scrape_time) >= 1")
    loan_json = [x for x in r]
    if len(loan_json) == 0:
        return
    all_data = None
    for loan_chunk in useful_functions.split_array_into_chunks(loan_json, 100):
        chunk_data = api.get_detailed_loan_data(loan_chunk)
        if all_data is None:
            all_data = dict()
            for key in chunk_data.keys():
                all_data[key] = []
        for key in all_data.keys():
            all_data[key] += chunk_data[key]
    db_connections.multi_table_upload(data=all_data, db=db, update=True, chunk_size=10000, ensure=True, ordered_tables=['loan'])


def upload_snapshot(db):
    q = """
replace into funding_trend (    loan_id,
    `status`,
    funded_amount,
    paid_amount,
    delinquent,
    lender_count,
    scrape_time)
SELECT
    loan.id AS loan_id,
    loan.`status`,
    loan.funded_amount,
    loan.paid_amount,
    loan.delinquent,
    loan.lender_count,
    loan.scrape_time
FROM
    loan
WHERE
    loan.`status` IN ('in_repayment' , 'inactive', 'fundraising')
    and scrape_time > (select max(scrape_time) from funding_trend)
    ;
    """
    db.query(q)


def run():
    db = db_connections.get_fungrosencrantz_schema('Kiva')
    api = kiva_api.KivaAPI()
    upload_new_loans_and_loan_lenders(db, api)
    update_current_projects(db, api)
    upload_missing_lenders(db, api)
    upload_snapshot(db)


if __name__ == '__main__':
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG, filename=log_file)
    logging.debug("Starting {}.".format(sys.argv[0]))
    try:
        run()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        try:
            useful_functions.send_scott_a_text(message='Kiva update failed')
        except Exception:
            logging.error('Message sending failed too!')
            logging.error(traceback.format_exc())
        raise
    logging.info('{} has completed.'.format(sys.argv[0]))
