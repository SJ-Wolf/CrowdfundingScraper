"""
This script downloads data from the kiva database and puts it into a xlsx file.

A few notes:
    For large amounts of data, excel will want to repair the file for whatever reason.
    This file cannot be moved around unless the lib folder and db_connections are still one folder up.
"""

import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import pandas as pd
import csv
import db_connections

file_name = '/tmp/kiva_lender_us_3'


def download(file_name):
    """

    :param start_date: Consider projects that have a start date after this date (format YYYY/MM/DD)
    :param end_date: Consider projects that have a start date before this date (format YYYY/MM/DD)
    :param file_name: Output file name.
    :return:
    """
    queries = {'loan': """
SELECT
    id,
    `name`,
    `status`,
    funded_amount,
    activity,
    sector,
    `use`,
    country_code,
    country,
    loan_amount,
    lender_count,
    posted_date,
    terms_disbursal_date,
    funded_date
FROM
    loan;""",
               'lender': """
SELECT
    lender_id,
    country_code,
    loan_count,
    member_since,
    uid,
    whereabouts
FROM
    lender;""",
               'loan_lender': """
SELECT
    *
FROM
    loan_lender;"""}

    db = db_connections.get_fungrosencrantz_schema('Kiva')

    for table in queries:
        print
        "downloading {}".format(table)
        r = db.query(queries[table])
        print
        "inserting into csv file"
        with open(file_name + '_' + table + '.csv', 'wb') as csv_file:
            for i, row in enumerate(r):
                utf8_row = dict()
                for key in row:
                    value = row[key]
                    if type(value) == unicode:
                        utf8_row[key] = value.encode('utf-8')
                if i == 0:
                    writer = csv.DictWriter(csv_file, fieldnames=utf8_row.keys())
                    writer.writeheader()
                writer.writerow(utf8_row)


def split_output_files(file_name):
    table = 'loan_lender'
    rows_per_csv = 500000  # 1000000
    with open(file_name + '.csv', 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        file_index = 0
        cur_output_array = []
        headers = csv_reader.next()
        for i, row in enumerate(csv_reader):
            cur_output_array.append(row)
            if i > 0 and (i + 1) % rows_per_csv == 0:
                with open('{}_{}.csv'.format(file_name, file_index), 'wb') as output_csv_file:
                    csv_writer = csv.writer(output_csv_file)
                    csv_writer.writerow(headers)
                    csv_writer.writerows(cur_output_array)
                    cur_output_array = []
                    file_index += 1
    if len(cur_output_array) > 0:
        with open('{}_{}.csv'.format(file_name, file_index), 'wb') as output_csv_file:
            csv_writer = csv.writer(output_csv_file)
            csv_writer.writerow(headers)
            csv_writer.writerows(cur_output_array)


def query_to_excel(query, chunk_size=10, outfile='output.xlsx', schema='Kiva'):
    from useful_functions import split_array_into_chunks
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter', options={'strings_to_urls': False})
    db = db_connections.get_fungrosencrantz_schema(schema)
    df = pd.read_sql(query, con=db.executable)
    print
    "done downloading"
    sheet_num = 0
    for df_chunk in split_array_into_chunks(df, chunk_size=chunk_size):
        sheet_num += 1
        if len(df_chunk) == 0:
            break
        df_chunk.to_excel(writer, sheet_name='sheet{}'.format(sheet_num))
    writer.save()


def download_us_lender_data():
    q = '''select
        `lender`.`lender_id` AS lender_id,
        `lender`.`country_code` AS lender_country_code,
        `lender`.`loan_count` AS lender_loan_count,
        `lender`.`member_since` AS lender_member_since,
        `lender`.`uid` AS lender_uid,
        `lender`.`whereabouts` AS lender_whereabouts
    FROM
        lender
    WHERE
        lender.country_code = "US"'''
    query_to_excel(q, chunk_size=1048574, outfile='kiva_lender_us.xlsx')


def download_us_loan_lender_data():
    q = '''SELECT
    loan_lender.*
FROM
    lender
        JOIN
    loan_lender
        ON lender.lender_id = loan_lender.lender_id
WHERE
    lender.country_code = 'US';'''
    query_to_excel(q, chunk_size=1048574, outfile='kiva_loan_lender_us.xlsx')


def download_us_loan_data():
    q = '''SELECT
    `loan`.`id` AS loan_id,
    `loan`.`name` AS loan_name,
    `loan`.`status` AS loan_status,
    `loan`.`funded_amount` AS loan_funded_amount,
    `loan`.`activity` AS loan_activity,
    `loan`.`sector` AS loan_sector,
    `loan`.`use` AS loan_use,
    `loan`.`country_code` AS loan_country_code,
    `loan`.`country` AS loan_country,
    `loan`.`loan_amount` AS loan_loan_amount,
    `loan`.`lender_count` AS loan_lender_count,
    `loan`.`posted_date` AS loan_posted_date,
    `loan`.`terms_disbursal_date` AS loan_terms_disbursal_date,
    `loan`.`funded_date` AS loan_funded_date
FROM
    loan
join(
SELECT
    loan_id
FROM
    lender
        JOIN
    loan_lender
        ON lender.lender_id = loan_lender.lender_id
WHERE
    lender.country_code = 'US'
group by loan_id) as t1
on t1.loan_id = loan.id'''
    query_to_excel(q, chunk_size=1048574, outfile='kiva_loan_us.xlsx')


if __name__ == '__main__':
    db = db_connections.get_fungrosencrantz_schema('kickstarter');
    commands = []
    schemas = [x['Database'] for x in db.query('show schemas')]
    for schema in schemas:
        db.query('use `{}`;'.format(schema))
        if schema in ('informational_schema', 'mysql', 'performance_schema', 'sys'):
            continue
        tables = [x['Tables_in_{}'.format(schema)] for x in db.query('show tables')]
        for table in tables:
            commands.append('optimize table `{}`.`{}`;'.format(schema, table))

    for c in commands:
        print
        c
