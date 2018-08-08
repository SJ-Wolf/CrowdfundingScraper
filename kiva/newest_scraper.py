#!/usr/bin/python

import json
import logging
import sqlite3
import sys
import traceback

import pandas as pd
from joblib import Parallel, delayed

from utils import useful_functions
# app ID = edu.berkeley.haas.crowdfunding.kiva
from kiva import kiva_api
from utils.sqlite_utils import insert_into_table, delete_temporary_tables


def get_most_recent_loan_in_database(db, table='loan'):
    """

    :param db: database connection to use
    :param table: table to look at in the default schema of the database
    :return: id number of the most recent loan (by posted date)
    """
    return "1094001"
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


def get_lenders_to_scrape(lender_table='lender', loan_lender_table='loan_lender'):
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
        --where not ({1}.lender_id > 0)
        group by {1}.lender_id
        having null_id is null;'''.format(lender_table, loan_lender_table)
    with sqlite3.connect('kiva.db') as db:
        cur = db.cursor()
        cur.execute(q)
        return [x[0] for x in cur.fetchall()]


def upload_missing_lenders_data(api):
    """
    Uploads any lenders that are missing from the database

    :param kiva_db: database kiva information is stored
    :param api: api to use (such as KivaAPI in kiva_api)
    :return: None
    """
    lenders_to_scrape = get_lenders_to_scrape()
    with Parallel(n_jobs=api.num_threads) as parallel:
        for big_loan_chunk in useful_functions.split_array_into_chunks(lenders_to_scrape, 10000):
            big_lender_chunk = []
            for lender_chunk in parallel(
                    delayed(api.get_lender_data)(loan_chunk) for loan_chunk in useful_functions.split_array_into_chunks(big_loan_chunk, 50)):
                big_lender_chunk += lender_chunk['lender']
            print('uploading big lender chunk')
            with sqlite3.connect('kiva.db') as db:
                df = pd.DataFrame(big_lender_chunk)
                insert_into_table(df, 'lender', db, replace=True)

    # all_lender_data = []
    # for lender_ids_chunk in useful_functions.split_array_into_chunks(data=lenders_to_scrape, chunk_size=50):
    #     all_lender_data += api.get_lender_data(lender_ids_chunk)['lender']
    # df = pd.DataFrame(all_lender_data)
    # with sqlite3.connect('kiva.db') as db:
    #     insert_into_table(df, 'lender', db, replace=True)


def get_new_loans_and_loan_lenders_data(api, stop_loan_id=84, from_file=None, update=True):
    """
    Uploads new loans and loan lenders to the database

    :param db: database to put new loan and loan_lender information in
    :param api: api to use (such as KivaAPI in kiva_api)
    :param from_file: uploads data from this file instead of querying the database to find what's missing
    :param update: whether to update database or insert missing data
    :return: None
    """
    newest_loans_json = api.get_most_recent_loans(stop_loan_id=stop_loan_id, per_page=100)

    all_data = None
    for loan_data_chunk in useful_functions.split_array_into_chunks(newest_loans_json, 100):
        loan_data = api.get_detailed_loan_data(loan_data_chunk)
        loans_lenders_data = api.get_loans_lenders_data(loan_data_chunk)
        total_data = dict(**loan_data, **loans_lenders_data)
        if all_data is None:
            all_data = total_data
        else:
            for key in all_data:
                all_data[key] += total_data[key]
    return all_data
    db_connections.multi_table_upload(all_data, db=db, update=update, ensure=True, ordered_tables=['loan'])


def upload_current_projects_updates(api):
    """
    Updates loans in the database that are still ongoing

    :param db: database to put new loan and loan_lender information in
    :param api: api to use (such as KivaAPI in kiva_api)
    :return: None
    """
    with sqlite3.connect('kiva.db') as db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        r = cur.execute("""
            select *
            from loan
            where `status` in ('in_repayment', 'inactive', 'fundraising')
                  and julianday(current_timestamp) - julianday(scrape_time) > 0.5 -- at least half a day""")
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
        for table_name in all_data:
            df = pd.DataFrame(all_data[table_name])
            insert_into_table(df, table_name, db, replace=True)


def upload_snapshot():
    with sqlite3.connect('kiva.db') as db:
        cur = db.cursor()
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
        cur.execute(q)


def update():
    api = kiva_api.KivaAPI(num_threads=1, make_cached_requests=False)
    upload_loan_details(api)
    upload_current_projects_updates(api)
    upload_missing_loan_lenders(api)
    upload_missing_lenders_data(api)
    upload_snapshot()
    delete_temporary_tables('kiva.db')


def upload_loan_details(api):
    with sqlite3.connect('kiva.db') as db:
        cur = db.cursor()
        cur.execute('select id from loan order by posted_date desc limit 100, 1')
        r = cur.fetchall()
        if len(r) == 0:
            stop_loan_id = None
        else:
            stop_loan_id = r[0][0]

    newest_loans_json = api.get_most_recent_loans(stop_loan_id=stop_loan_id, per_page=100)
    logging.debug("Starting parallel download")
    # with Parallel(n_jobs=6) as parallel:
    #     for big_loan_chunk in useful_functions.split_array_into_chunks(newest_loans_json, 10000):
    #         parallel(delayed(api.get_detailed_loan_data)(loan_chunk) for loan_chunk in useful_functions.split_array_into_chunks(big_loan_chunk, 100))
    for big_loan_chunk in useful_functions.split_array_into_chunks(newest_loans_json, 10000):
        all_data = None
        for loan_data_chunk in useful_functions.split_array_into_chunks(big_loan_chunk, 100):
            print('done chunk...')
            loan_data = api.get_detailed_loan_data(loan_data_chunk)
            # loans_lenders_data = api.get_loans_lenders_data(loan_data_chunk)
            # total_data = dict(**loan_data, **loans_lenders_data)
            total_data = loan_data
            if all_data is None:
                all_data = total_data
            else:
                for key in all_data:
                    all_data[key] += total_data[key]
        with sqlite3.connect('kiva.db') as db:
            print('done BIG chunk...')
            db.executescript("""
                PRAGMA page_size = 4096;
                PRAGMA cache_size = 2652524;
                PRAGMA temp_store = MEMORY
                """)
            for table_name in all_data:
                df = pd.DataFrame(all_data[table_name])
                insert_into_table(df, table_name, db, replace=True)


def upload_missing_loan_lenders(api):
    with sqlite3.connect('kiva.db') as db:
        cur = db.cursor()
        cur.execute('''
            select id
            from loan
            where lender_count > 0 
            and not exists(select 1
                             from loan_lender
                             where loan_lender.loan_id = loan.id)''')
        missing_loan_ids = [x[0] for x in cur.fetchall()]
        for loan_id_chunk in useful_functions.split_array_into_chunks(missing_loan_ids, chunk_size=1000):
            loan_lenders_data = []
            for loan_id in loan_id_chunk:
                lender_ids = api.get_loans_lenders(loan_id)
                for lender_id in lender_ids:
                    loan_lenders_data.append((loan_id, lender_id))
            cur.executemany('insert or ignore into loan_lender values (?, ?)', loan_lenders_data)
            db.commit()


if __name__ == '__main__':
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG, filename=log_file)
    logging.debug("Starting {}.".format(sys.argv[0]))
    try:
        update()
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
