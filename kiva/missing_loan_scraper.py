import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import db_connections
import logging
import useful_functions
import kiva_api
import traceback
import json
import shutil
from newest_scraper import *
from kiva_api import KivaAPI


def get_all_loans_in_database(db, table='loan'):
    """

    :param db: database to select from
    :param table: table loans are located in
    :return:
    """

    return [x['id'] for x in db.query('select id from loan')]


def dump_kiva_info():
    loans_on_kiva_location = dict()
    all_loans_on_kiva = []
    for i in range(1, 2115 + 1):
        print
        i
        cur_file = 'to_be_analyzed/loans/{}.json'.format(i)
        with open(cur_file) as f:
            loan_json = json.load(f)
        for loan in loan_json:
            loans_on_kiva_location[str(loan['id'])] = cur_file
        new_loan_ids = [x['id'] for x in loan_json]
        all_loans_on_kiva += new_loan_ids
    with open('loans_on_kiva_location.json', 'w') as f:
        json.dump(loans_on_kiva_location, f)
    with open('all_loans_on_kiva.json', 'w') as f:
        json.dump(all_loans_on_kiva, f)


def dump_missing_loan_data(kiva_db, loan_table='loan', output_file_name='to_be_analyzed/loans/1.json'):
    # with open('all_loans_in_database.json') as f:
    #    all_loans_in_database = json.load(f)
    all_loans_in_database = [x['id'] for x in db.query('select id from loan')]
    with open('all_loans_on_kiva.json') as f:
        all_loans_on_kiva = json.load(f)
    with open('loans_on_kiva_location.json') as f:
        text = f.read()
        text = text.replace('to_be_analyzed/loans/', 'all_kiva_loans/loans/')
        loans_on_kiva_location = json.loads(text)

    all_loans_in_database = set(all_loans_in_database)
    all_loans_on_kiva = set(all_loans_on_kiva)
    missing_loans = all_loans_on_kiva - all_loans_in_database

    files_with_missing_loans = []
    for loan in list(missing_loans):
        files_with_missing_loans.append(loans_on_kiva_location[str(loan)])
    files_with_missing_loans = set(files_with_missing_loans)

    missing_loan_data = dict()
    missing_loan_data['header'] = dict(total=len(missing_loans),
                                       page=1,
                                       date="2016-06-08T23:00:00Z",
                                       page_size=len(missing_loans))
    missing_loan_data['loans'] = []
    for loan_file in list(files_with_missing_loans):
        with open(loan_file) as f:
            loan_data = json.load(f)
        for loan in loan_data:
            if int(loan['id']) in missing_loans:
                missing_loan_data['loans'].append(loan)

    with open(output_file_name, 'w') as f:
        json.dump(missing_loan_data['loans'], f)


def run():
    db = db_connections.get_fungrosencrantz_schema('Kiva')
    api = KivaAPI()
    upload_new_loans_and_loan_lenders(db, api, from_file='missing_loan_data.json', update=False)


if __name__ == '__main__':
    # log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting {}.".format(sys.argv[0]))
    db = db_connections.get_fungrosencrantz_schema('Kiva')
    try:
        # dump_missing_loan_data(kiva_db=db, output_file_name='missing_loan_data.json')
        run()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        raise
    logging.info('{} has completed.'.format(sys.argv[0]))
