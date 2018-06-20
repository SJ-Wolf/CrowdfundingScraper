# TODO: update to local database

import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import pandas as pd
import db_connections
import logging
import traceback
import numpy as np
from scipy.interpolate import interp1d
from currency_converter import CurrencyConverter
import json
import os


def convert_kickstarter_currency_df_to_usd(df, replace=True):
    currency_converter = CurrencyConverter()

    def convert_amount_pledged_to_usd(row):
        return currency_converter.convert(
            amount=row['amount_pledged'],
            currency=row['currency'],
            new_currency='USD')

    def convert_goal_to_usd(row):
        return currency_converter.convert(
            amount=row['goal'],
            currency=row['currency'],
            new_currency='USD')

    if replace:
        df['amount_pledged'] = df.apply(convert_amount_pledged_to_usd, axis=1)
        df['goal'] = df.apply(convert_goal_to_usd, axis=1)
    else:
        df['amount_pledged_usd'] = df.apply(convert_amount_pledged_to_usd, axis=1)
        df['goal_usd'] = df.apply(convert_goal_to_usd, axis=1)


def get_aggregate_funding_trend_df(status='successful', min_data_points=4, limit=None):
    db = db_connections.get_fungrosencrantz_schema(schema='kickstarter')
    sql = """
SELECT
    projectid,
    funding_trend.amount_pledged,
    project.currency,
    funding_trend.update_count,
    funding_trend.comment_count,
    funding_trend.backer_count,
    DATEDIFF(date_added, start_date) AS `day`,
    goal
FROM
    funding_trend
        JOIN
    project ON funding_trend.projectid = project.id
WHERE
    project.status = "{}"
ORDER BY projectid
{}
    """.format(status, 'limit {}'.format(limit) if limit is not None else "")
    logging.info('Acquiring data')
    complete_dataframe = pd.read_sql(sql, db.executable)

    logging.info('Converting currencies to USD')
    convert_kickstarter_currency_df_to_usd(complete_dataframe)
    complete_dataframe['percent_of_goal'] = np.divide(complete_dataframe['amount_pledged'], complete_dataframe['goal']) * 100
    del complete_dataframe['currency']

    # add interpolation on the various columns per project_id
    logging.info('Interpolating...')
    grouped = complete_dataframe.groupby('projectid')
    filled_df = None
    for projectid, sub_df in grouped:
        if 0 not in sub_df['day'].values:
            day_0_df = pd.DataFrame([[projectid, 0, 0, 0, 0, 0, sub_df['goal'].values[0], 0]], columns=sub_df.columns)
            sub_df = day_0_df.append(sub_df)
        if len(sub_df) <= min_data_points:
            continue
        interp_functions = dict(
            percent_of_goal=interp1d(sub_df['day'], sub_df['percent_of_goal']),
            amount_pledged=interp1d(sub_df['day'], sub_df['amount_pledged']),
            update_count=interp1d(sub_df['day'], sub_df['update_count']),
            comment_count=interp1d(sub_df['day'], sub_df['comment_count']),
            backer_count=interp1d(sub_df['day'], sub_df['backer_count'])
        )
        data = dict()
        num_days = sub_df['day'].max() + 1
        days_to_evaluate_for = range(num_days)
        data['projectid'] = pd.Series([projectid] * num_days)
        data['amount_pledged'] = pd.Series(interp_functions['amount_pledged'](days_to_evaluate_for))
        data['update_count'] = pd.Series([int(round(x)) for x in interp_functions['update_count'](days_to_evaluate_for)])
        data['comment_count'] = pd.Series([int(round(x)) for x in interp_functions['comment_count'](days_to_evaluate_for)])
        data['backer_count'] = pd.Series([int(round(x)) for x in interp_functions['backer_count'](days_to_evaluate_for)])
        data['day'] = pd.Series(days_to_evaluate_for)
        data['goal'] = pd.Series([sub_df['goal'].values[0]] * num_days)
        data['percent_of_goal'] = pd.Series(interp_functions['percent_of_goal'](days_to_evaluate_for))
        partial_filled_df = pd.DataFrame.from_dict(data=data)
        if filled_df is None:
            filled_df = partial_filled_df
        else:
            filled_df = filled_df.append(partial_filled_df)

    del filled_df['projectid']
    del filled_df['goal']

    logging.info('Grouping and taking median')
    grouped = filled_df.groupby('day')
    grouped_df = grouped.median()
    grouped_df['count'] = grouped.size()

    return grouped_df

    '''
    print grouped_df

    logging.info('Outputting to Excel')
    writer = pd.ExcelWriter('{}_project_analysis.xlsx'.format(status), engine='xlsxwriter')
    complete_dataframe.to_excel(writer, sheet_name='complete')
    filled_df.to_excel(writer, sheet_name='interpolated')
    grouped_df.to_excel(writer, sheet_name='grouped')

    workbook = writer.book
    worksheet = writer.sheets['grouped']
    max_row = len(grouped_df)
    chart = workbook.add_chart({'type': 'scatter'})
    chart.add_series({
        'name': ['grouped', 0, 6],
        'categories': ['grouped', 1, 0, max_row, 0],
        'values': ['grouped', 1, 6, max_row, 6],
        'marker': {'type': 'circle', 'size': 7},
    })

    # Configure the chart axes.
    chart.set_x_axis({'name': 'Day'})
    chart.set_y_axis({'name': 'count',
                      'major_gridlines': {'visible': False}})

    # Insert the chart into the worksheet.
    worksheet.insert_chart('I2', chart)

    for sheet_name in interp_functions.keys():
        df = grouped_df[[sheet_name, 'count']]
        df.to_excel(writer, sheet_name=sheet_name)
        chart = workbook.add_chart({'type': 'scatter'})
        worksheet = writer.sheets[sheet_name]
        worksheet.set_column('A:A', 3.29)
        worksheet.set_column('B:B', 20)
        max_row = len(df)
        chart.add_series({
            'name': [sheet_name, 0, 1],
            'categories': [sheet_name, 1, 0, max_row, 0],
            'values': [sheet_name, 1, 1, max_row, 1],
            'marker': {'type': 'circle', 'size': 7},
        })

        # Configure the chart axes.
        chart.set_x_axis({'name': 'Day'})
        chart.set_y_axis({'name': sheet_name,
                          'major_gridlines': {'visible': False}})

        # Insert the chart into the worksheet.
        worksheet.insert_chart('E2', chart)

    writer.save()
    '''


def run():
    from useful_functions import ensure_directory
    sql = "select id as project_id, goal, amount_pledged, backer_count, category, year(start_date) as start_year, currency from project  where currency is not Null"
    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    df = pd.read_sql(sql=sql, con=db.executable)
    convert_kickstarter_currency_df_to_usd(df, replace=True)
    base_directory = '/usr/share/nginx/html/crowdfunding/my/scott/kickstarter/goal_vs_actual'
    original_directory = os.getcwd()
    os.chdir(base_directory)
    data_directory = 'analysis_output'

    # def start_date_to_str(row):
    #    return row['start_date'].isoformat()
    # df['start_date'] = df.apply(start_date_to_str, axis=1)
    mapping = dict()
    for year_category, year_category_df in df.groupby(['start_year', 'category']):
        year = year_category[0]
        category = year_category[1]
        cur_directory = '{}/{}'.format(data_directory, year)
        ensure_directory(cur_directory)
        output_file = '{}/{}.json'.format(cur_directory, category)
        if year not in mapping.keys():
            mapping[year] = dict()
        mapping[year][category] = output_file
        with open(output_file, 'w') as f:
            json.dump(year_category_df.to_dict(orient='records'), f)

    ensure_directory(data_directory)
    with open('{}/mapping.json'.format(data_directory), 'w') as f:
        json.dump(mapping, f)
    os.chdir(original_directory)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting analysis.")
    try:
        run()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        raise
    logging.info('Analysis has completed.')
