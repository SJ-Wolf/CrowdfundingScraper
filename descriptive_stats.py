# TODO: update to local database

import decimal
import logging
import os

import pandas as pd
from currency_converter import CurrencyConverter, RateNotFoundError

from unused_scripts import db_connections


def get_kickstarter_stats(db):
    sql = """
        SELECT
          datediff(end_date, start_date) AS `Duration`,
          backer_count                   AS `Backer count`,
          comment_count                  AS `Comment count`,
          has_video                      AS `Has video`,
          body_length                    AS `Body length`,
          goal                           AS `Goal`,
          amount_pledged                 AS `Amount pledged`,
          currency                       AS `Currency`,
          date(start_date)               AS `Start date`
        FROM kickstarter.project;
    """
    get_descriptive_stats(db=db, sql=sql, outfile_name='kickstarter', variables=[
        dict(name='Duration',
             units='days'),
        dict(name='Backer count'),
        dict(name='Comment count'),
        dict(name='Has video'),
        dict(name='Body length',
             units='characters'),
        dict(name='Goal',
             units='$',
             is_currency=True,
             type_of_currency='Currency',
             currency_date='Start date'),
        dict(name='Amount pledged',
             units='$',
             is_currency=True,
             type_of_currency='Currency',
             currency_date='Start date'),
    ])


def get_kiva_stats(db):
    sql = """
    SELECT
      funded_amount AS `Funded amount`,
      paid_amount   AS `Repaid amount`,
      lender_count  AS `Lender count`,
      loan_amount   AS `Loan amount`
    FROM Kiva.loan;"""
    get_descriptive_stats(db=db, sql=sql, outfile_name='kiva', variables=[
        dict(name='Funded amount',
             units='$'),
        dict(name='Repaid amount',
             units='$'),
        dict(name='Loan amount',
             units='$'),
        dict(name='Lender count'),
    ])


def run():
    logging.basicConfig(level=logging.DEBUG)
    os.chdir('kickstarter')
    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    # get_kiva_stats(db)
    get_kickstarter_stats(db)


exp_map = dict()


def get_scaled_nums(nums):
    results = []
    global exp_map
    if len(exp_map) == 0:
        logging.debug('Initializing exponential map')
        exp_map[0] = None
        exp_map[3] = '1000s'
        exp_map[6] = 'Millions'
        exp_map[9] = 'Billions'

    for exp in exp_map.keys():
        bad_nums = 0
        nice_strings = []
        for num in nums:
            d = decimal.Decimal(num / (10 ** exp)).normalize(decimal.Context(prec=4))
            if 1000000 > d >= 1:
                d_str = '{:,f}'.format(d)
            else:
                d_str = d.to_eng_string()
            nice_strings.append(d_str)
            if d < .1 or d >= 1000 or 'E' in d_str:
                bad_nums += 1
        results.append((exp, bad_nums, nice_strings))

    best_exp = None
    best_bad_nums = float('Inf')
    for exp, bad_nums, nice_strings in results:
        if bad_nums < best_bad_nums:
            best_bad_nums = bad_nums
            best_exp = exp
            best_nice_strings = nice_strings

    return exp_map[best_exp], best_nice_strings


def get_descriptive_stats(db, sql, outfile_name, variables):
    """

    :param db: dataset database
    :param sql: data from this sql statement is analyzed
    :param outfile_name: output files begin with outfile_name
    :param variables: list of dictionaries, each describing a variable
        dictionary keys:
            name: name of the variable; must match sql statement
            units: units of the variable to display (default '')
            is_currency: if it's a currency variable (default False)
            type_of_currency: variable in the sql statement that describes its currency (e.g. 'USD') (default 'USD')
            currency_date: variable in the sql statement that describes the date to get the exchange rate for
                (default is current)

    :return: returns None; outputs two files - $outfile_name$_desc_stats.latex and $outfile_name$_desc_stats.csv
    """
    currency_converter = CurrencyConverter()

    def convert_col_to_usd(row, amount_col, currency_col, date_col=None):
        if pd.isnull(row[amount_col]):
            return row[amount_col]
        if date_col is None:
            return currency_converter.convert(
                amount=row[amount_col],
                currency=row[currency_col],
                new_currency='USD'
            )
        else:
            try:
                return currency_converter.convert(
                    amount=row[amount_col],
                    currency=row[currency_col],
                    new_currency='USD',
                    date=row[date_col]
                )
            except RateNotFoundError:
                return currency_converter.convert(
                    amount=row[amount_col],
                    currency=row[currency_col],
                    new_currency='USD'
                )

    logging.debug('Reading sql...')
    df = pd.read_sql(sql=sql, con=db.executable)

    logging.debug('Converting any currencies...')
    for var in variables:
        if var.get('is_currency'):
            df[var['name']] = df.apply(func=convert_col_to_usd, axis=1,
                                       args=(
                                           var['name'],
                                           var.get('type_of_currency'),
                                           var.get('currency_date')
                                       ))

    logging.debug('Getting stats...')
    desc_df = df.describe(percentiles=[])
    desc_df = desc_df.transpose()

    desc_df['median'] = df.median()
    desc_df['sum'] = df.sum()
    desc_df.rename(columns={'std': 'std dev'}, inplace=True)
    del desc_df['count']
    del desc_df['50%']
    desc_df = desc_df[['mean', 'median', 'min', 'max', 'std dev', 'sum']]

    desc_df = desc_df.transpose()

    for var in variables:
        column_name = var['name']
        unit, scaled_nums = get_scaled_nums(nums=desc_df[column_name])
        desc_df[column_name] = scaled_nums
        if unit is None and var.get('units') is None:
            pass  # leave name alone
        elif unit is None and var.get('units') is not None:
            desc_df.rename(columns={column_name: '{} ({})'.format(column_name, var['units'])}, inplace=True)
        elif unit is not None and var.get('units') is None:
            desc_df.rename(columns={column_name: '{} ({})'.format(column_name, unit)}, inplace=True)
        elif unit is not None and var.get('units') is not None:
            desc_df.rename(columns={column_name: '{} ({} of {})'.format(column_name, unit, var['units'])}, inplace=True)

    desc_df = desc_df.transpose().sort()

    with open('../{}_desc_stats.latex'.format(outfile_name), 'w') as f:
        f.write(desc_df.to_latex())
    desc_df.to_csv('../{}_desc_stats.csv'.format(outfile_name))

    print()
    desc_df.to_latex()
    print()
    desc_df


if __name__ == '__main__':
    run()
