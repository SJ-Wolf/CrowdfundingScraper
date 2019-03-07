import time
from kiva import newest_scraper
import sys
import logging
from utils import useful_functions
import traceback
from kickstarter import kickstarter_updater
from utils.sqlite_utils import get_create_table_statements
import sqlite3


def try_run_function(f, failed_text):
    try:
        f()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        try:
            useful_functions.send_scott_a_text(message=failed_text)
        except Exception:
            logging.error('Message sending failed too!')
            logging.error(traceback.format_exc())


def sql_create_statements_to_disk(database_names: list):
    """
    Note: be sure to include .db in the database names
    :param database_names:
    :return:
    """
    for db_name in database_names:
        sql = '\n'.join(get_create_table_statements(db_name))
        with open(f'create_tables_{db_name}.sql', 'w') as f:
            f.write(sql)


def create_new_databases(database_names: list):
    """
    Create empty databases from previously saved create statements.
    :param database_names: e.g. ["kiva.db", "kickstarter.db"]
    :return:
    """
    for db_name in database_names:
        with sqlite3.connect(db_name) as db:
            cur = db.cursor()
            with open(f'create_tables_{db_name}.sql') as f:
                cur.executescript(f.read())


if __name__ == '__main__':
    t0 = time.time()
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG, filename=log_file)
    logging.debug("Starting {}.".format(sys.argv[0]))

    try_run_function(newest_scraper.update, 'Kiva update failed!')
    try_run_function(kickstarter_updater.update, 'Kickstarter update failed!')

    logging.info('{} has completed.'.format(sys.argv[0]))
    print(time.time() - t0)
