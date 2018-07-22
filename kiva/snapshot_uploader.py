import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')

import os
import json
import logging
# import db_connections
import shutil
from kiva.kiva_json_analyzer import analyze_loans_lenders_data, analyze_loans_data, analyze_lenders_data
from utils.file_utils import enter_folder
import csv
import sqlite3
import pandas as pd


def run():
    # db = db_connections.get_fungrosencrantz_schema(schema='Kiva')
    for category in ('lenders', 'loans_lenders', 'loans'):
        for root, dirs, files in os.walk('to_be_analyzed/' + category):
            for i, file_name in enumerate(files):
                if file_name.endswith(".json"):
                    cur_file = os.path.join(root, file_name)
                    logging.info('Current file: ' + cur_file)
                    with open(cur_file) as f:
                        cur_file_json = json.load(f)
                    # cur_file_list = cur_file_json[category]
                    if category == 'loans_lenders':
                        data = analyze_loans_lenders_data(cur_file_json)
                    elif category == 'loans':
                        data = analyze_loans_data(cur_file_json, scrape_time=cur_file_json['header']['date'])
                    elif category == 'lenders':
                        data = analyze_lenders_data(cur_file_json)
                    else:
                        raise Exception("Unknown category")
                    # db_connections.multi_table_upload(data=data, db=db, update=True, strict=True)
                    shutil.move(src=cur_file, dst=cur_file.replace('to_be_analyzed/', 'done_analyzing/'))
                    # if i > -1: break  # only want to deal with one for now
                print("{}% DONE".format(100. * (i + 1) / len(files)))


def split_csv_col(file_name, index_cols, split_cols, skip_existing):
    out_file_name = file_name[:file_name.rindex('.')] + '_split' + file_name[file_name.rindex('.'):]
    if not skip_existing or not os.path.exists(out_file_name):
        with open(file_name, 'r', encoding='utf8') as f_in:
            with open(out_file_name, 'w', encoding='utf8') as f_out:
                dict_reader = csv.DictReader(f_in)
                dict_writer = csv.DictWriter(f_out, [*index_cols, *split_cols], delimiter=',', lineterminator='\n')
                dict_writer.writeheader()
                for line in dict_reader:
                    split_row = ((x.strip() for x in line[col].split(',')) for col in split_cols)
                    for sub_row in zip(*split_row):
                        d = dict()
                        for col in index_cols:
                            d[col] = line[col]
                        for col, val in zip(split_cols, sub_row):
                            d[col] = val
                        dict_writer.writerow(d)
    return out_file_name


def csv_to_sql_table(file_name, db, table_name=None, index_cols=None, split_col_names=None, split_cols=None):
    if split_cols is None:
        split_cols = []
    if split_col_names is None:
        split_col_names = []
    assert len(split_col_names) == len(split_cols)
    db.executescript("""
        PRAGMA page_size = 4096;
        PRAGMA cache_size = 2652524;
        PRAGMA temp_store = MEMORY
        """)
    for name, columns in zip(split_col_names, split_cols):
        csv_to_sql_table(split_csv_col(file_name, index_cols, columns, skip_existing=False), db, name, index_cols=index_cols + columns)

    if table_name is None:
        return
    df = pd.read_csv(file_name, index_col=index_cols)
    # df.rename(str.lower, axis='columns', inplace=True)
    t0 = time.time()
    df.to_sql(name=table_name, con=db, if_exists='replace', index=True, )
    print(time.time() - t0)


def csv_uploader(data_dir):
    with enter_folder(data_dir):
        with sqlite3.connect('kiva.db') as db:
            csv_to_sql_table('lenders.csv', db, 'lender', index_cols=['LOAN_ID'])
            csv_to_sql_table('loans_lenders.csv', db, index_cols=['LOAN_ID'], split_col_names=['loan_lender'], split_cols=[['LENDERS']])
            csv_to_sql_table('loans.csv', db, 'loan', index_cols=['LOAN_ID'], split_col_names=['tag', 'borrower'],
                             split_cols=[['TAGS'], ['BORROWER_NAMES', 'BORROWER_GENDERS', 'BORROWER_PICTURED']])


if __name__ == "__main__":
    logging.basicConfig(level='INFO')
    import time

    t0 = time.time()
    csv_uploader('snapshot')
    print(time.time() - t0)
