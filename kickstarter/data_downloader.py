"""
This script downloads project, reward, and update data from the kickstarter database and puts it into a xlsx file.
start_date, end_date, and file_name can be changed according to requirements.

A few notes:
    It takes projects where the start date of the projects is between start_date and end_date.
    For large amounts of data, excel will want to repair the file for whatever reason.
    This file cannot be moved around unless the lib folder and db_connections are still one folder up.
"""

import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import pandas as pd
import db_connections

START_DATE = '2015-01-01'
END_DATE = '2015-12-31'
FILE_NAME = 'kickstarter_2015'


def run(start_date, end_date, file_name):
    """

    :param start_date: Consider projects that have a start date after this date (format YYYY/MM/DD)
    :param end_date: Consider projects that have a start date before this date (format YYYY/MM/DD)
    :param file_name: Output file name.
    :return:
    """
    queries = {'project': """select * from project left join location on project.location_slug = location.slug
    where start_date between {} and {};""".format(start_date, end_date),
               'reward': """select * from reward join (project left join location on project.location_slug = location.slug) on reward.projectid = project.id
    where start_date between {} and {};""".format(start_date, end_date),
               'update': """select * from `update` join (project left join location on project.location_slug = location.slug) on `update`.projectid = project.id
    where start_date between {} and {};""".format(start_date, end_date)}

    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    writer = pd.ExcelWriter(file_name + '.xlsx', engine='xlsxwriter', options={'strings_to_urls': False})

    for table in queries:
        print
        "downloading {}".format(table)
        df = pd.read_sql(queries[table], db.executable)
        print
        "inserting into spreadsheet"
        df.to_excel(writer, table)
    print
    "saving spreadsheet"
    writer.save()


if __name__ == '__main__':
    run(START_DATE, END_DATE, FILE_NAME)
