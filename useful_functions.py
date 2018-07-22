import csv
import math
import os
import smtplib
from decimal import Decimal, Context
from email.mime.text import MIMEText

import numpy as np
import pandas as pd
from currency_converter import CurrencyConverter, RateNotFoundError


def fast_insert_many(data, table, cur):
    with open('tmp.csv', 'w', newline='') as f:
        raw_files_writer = csv.writer(f, delimiter='\x0e', escapechar='\\', lineterminator='\n')
        raw_files_writer.writerows(data)
    with open('tmp.csv', 'r') as f:
        cur.copy_from(f, table, sep='\x0e', null='', size=32000)
    os.remove('tmp.csv')


def combine_list_of_dicts_on_column(primary_data, secondary_data, key):
    combination_data = []
    for primary_row in primary_data:
        for secondary_row in secondary_data:
            if primary_row[key] == secondary_row[key]:
                combination_data.append({**secondary_row, **primary_row})
                break
    return combination_data
    # df1 = pd.DataFrame.from_records(primary_data, index=[key])
    # df2 = pd.DataFrame.from_records(secondary_data, index=[key])
    # df = df1.join(df2, how='inner', rsuffix='!!!!!')
    # df.drop([x for x in df.columns if x.endswith('!!!!!')], axis=1, inplace=True)
    # df[key] = df.index
    # return df.to_dict(orient='records')


def split_array_into_chunks(data, chunk_size=50):
    i = 0
    while i + chunk_size < len(data):
        yield data[i:i + chunk_size]
        i += chunk_size
    yield data[i:]


def chunks(iterator, chunk_size):
    results = []
    for i, row in enumerate(iterator):
        results.append(row)
        if i % chunk_size == chunk_size - 1:
            yield results
            results = []
    if len(results) != 0:
        return results


def ensure_directory(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def add_column_to_list_of_dictionaries(data, column, value):
    for i in range(len(data)):
        data[i][column] = value


def rename_column_in_list_of_dictionaries(data, old_column, new_column):
    for i in range(len(data)):
        data[i][new_column] = data[i].pop(old_column)


def send_scott_a_text(message, subject=''):
    me = 'kyle1940@gmail.com'
    you = '6127593039@vtext.com'

    msg = MIMEText(message)
    # me == the sender's email address
    # you == the recipient's email address
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = you

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    s = smtplib.SMTP('localhost')
    s.sendmail(me, [you], msg.as_string())
    s.quit()


def remove_chars_from_string(string, chars_to_remove):
    if type(string) == str:
        return string.translate(None, chars_to_remove)
    return "".join([z for z in string if z not in chars_to_remove])


def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [alist[i * length // wanted_parts: (i + 1) * length // wanted_parts] for i in range(wanted_parts)]


def get_file_names_from_url_file(infile):
    with open(infile) as f:
        if os.name == 'nt':
            return ['E:\\' + x[:-1].replace('https://', '').replace('/', '\\') for x in f.readlines()]
        else:
            return [x[:-1].replace('https://', '') for x in f.readlines()]


prefixes = dict()


def round_to_sig_figs(x, sig_figs=1):
    return round(x, -int(math.floor(math.log10(abs(x)))) + sig_figs - 1)


def ceil_to_first_sig_fig(x):
    return (int(str(x)[0]) + 1) * 10 ** int(math.floor(math.log10(abs(x))))


def get_major_minor_ticks(min_num, max_num, major_freq, rel_minor_freq):
    return np.arange(min_num, max_num + 1, max_num / major_freq), np.arange(min_num, max_num + 1, max_num / major_freq / rel_minor_freq)


def pretty_number_format(num, short=False):
    if num == 0.:
        return '0.0'

    global prefixes
    if len(prefixes) == 0:
        logging.debug('initializing prefixes dictionary')
        prefixes['-12'] = ('Trillionths', 'Trillionths')
        prefixes['-9'] = ('Billionths', 'Billionths')
        prefixes['-6'] = ('Millionths', 'Millionths')
        prefixes['-3'] = ('Thousandths', 'Thousandths')
        prefixes['0'] = ('', '')
        prefixes['+3'] = ('Thousand', 'Th')
        prefixes['+6'] = ('Million', 'Mi')
        prefixes['+9'] = ('Billion', 'Bi')
        prefixes['+12'] = ('Trillion', 'Tr')

    eng_str = Decimal(num).normalize(Context(prec=4)).to_eng_string()
    # return eng_str
    e_pos = eng_str.find('E')
    if e_pos == -1:
        return eng_str
    else:
        postfix = eng_str[e_pos + 1:]
        if short:
            return eng_str[:e_pos] + ' ' + prefixes[postfix][1]
        else:
            return eng_str[:e_pos] + ' ' + prefixes[postfix][0]


def to_precision(x, p):
    """
    returns a string representation of x formatted with a precision of p
    Based on the webkit javascript implementation taken from here:
    https://code.google.com/p/webkit-mirror/source/browse/JavaScriptCore/kjs/number_object.cpp
    """

    x = float(x)

    if x == 0.:
        return "0." + "0" * (p - 1)

    out = []

    if x < 0:
        out.append("-")
        x = -x

    e = int(math.log10(x))
    tens = math.pow(10, e - p + 1)
    n = math.floor(x / tens)

    if n < math.pow(10, p - 1):
        e = e - 1
        tens = math.pow(10, e - p + 1)
        n = math.floor(x / tens)

    if abs((n + 1.) * tens - x) <= abs(n * tens - x):
        n = n + 1

    if n >= math.pow(10, p):
        n = n / 10.
        e = e + 1

    m = "%.*g" % (p, n)

    if e < -2 or e >= p:
        out.append(m[0])
        if p > 1:
            out.append(".")
            out.extend(m[1:p])
        out.append('e')
        if e > 0:
            out.append("+")
        out.append(str(e))
    elif e == (p - 1):
        out.append(m)
    elif e >= 0:
        out.append(m[:e + 1])
        if e + 1 < len(m):
            out.append(".")
            out.extend(m[e + 1:])
    else:
        out.append("0.")
        out.extend(["0"] * -(e + 1))
        out.append(m)

    return "".join(out)


"""
def get_descriptive_stats_latex(db, variables=[], table=''):
    q = ''
    for i, var in enumerate(variables):
        if i != 0:
            q += '\nunion\n'
        q += '''
select "{0}" as `variable`, count({0}) as N,
    avg({0}) as `mean`, std({0}) as `sd`,
    min({0}) as `min`, max({0}) as `max`
    FROM {1}
        '''.format(var, table)
    df = pd.read_sql(sql=q, con=db.executable, index_col='variable')
    df = df.applymap(lambda x: to_precision(x, 3))
    print df
    print '\n'
    print df.to_latex()
"""


def convert_currency(df, currency_column, money_column, date_column=None):
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

    currency_converter = CurrencyConverter()
    df[money_column] = df.apply(func=convert_col_to_usd, axis=1, args=(money_column, currency_column, date_column))


def get_descriptive_stats_latex(db, outfile_name, variables=[], table='', currency_variables=[], manual=False):
    logging.info('Getting {} descriptive stats'.format(table))
    if len(currency_variables) == 0 and not manual:
        q = ''
        for i, var in enumerate(variables):
            if i != 0:
                q += '\nunion\n'
            q += '''
        select "{0}" as `variable`, count({0}) as `count`,
            avg({0}) as `mean`, std({0}) as `std`,
            min({0}) as `min`, max({0}) as `max`
            FROM {1}
                '''.format(var, table)
        df = pd.read_sql(sql=q, con=db.executable, index_col='variable')
        df = df.applymap(lambda x: to_precision(x, 3))
        print(df)
        print('\n')
        logging.info(df.to_latex())
        df.to_csv('../{}_desc_stats.csv'.format(table))
    else:
        variables = list(set(variables + [x['variable'] for x in currency_variables]))
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

        additional_required_column = []
        for currency_var_dict in currency_variables:
            additional_required_column += [currency_var_dict['variable'],
                                           currency_var_dict['currency_column'],
                                           currency_var_dict['date_column']]
        additional_required_column = list(set(additional_required_column))

        q = 'select  '
        for i, var in enumerate(set(variables + additional_required_column)):
            if i != 0:
                q += ', '
            q += var
        q += ' from {} limit 1000;'.format(table)

        logging.debug('Reading sql...')
        df = pd.read_sql(sql=q, con=db.executable)

        logging.debug('Converting any currencies...')
        for currency_var_dict in currency_variables:
            column = currency_var_dict['variable']
            currency_column = currency_var_dict['currency_column']
            date_column = currency_var_dict['date_column']
            df[column] = df.apply(func=convert_col_to_usd, axis=1, args=(column, currency_column, date_column))

        for not_required_column in set(additional_required_column) - set(variables):
            del df[not_required_column]

        logging.debug('Getting stats...')
        desc_df = df.describe(percentiles=[])

        logging.debug('Converting to nicer format...')
        desc_df = desc_df.transpose()
        desc_df['median'] = df.median()
        desc_df['sum'] = df.sum()
        desc_df.rename(columns={'std': 'std dev'}, inplace=True)
        del desc_df['count']
        desc_df.index = desc_df.index.map(lambda x: x[0].upper() + x[1:].replace('_', ' '))
        desc_df = desc_df.applymap(lambda x: pretty_number_format(x, False) if pd.notnull(x) else x).sort()
        del desc_df['50%']
        desc_df = desc_df[['mean', 'median', 'min', 'max', 'std dev', 'sum']]

        with open('../{}_desc_stats.latex'.format(outfile_name), 'w') as f:
            f.write(desc_df.to_latex())
        desc_df.to_csv('../{}_desc_stats.csv'.format(outfile_name))

        print(desc_df.to_latex())
        print(desc_df)


if __name__ == '__main__':
    import db_connections
    import logging

    logging.basicConfig(level=logging.DEBUG)
    os.chdir('kickstarter')
    db = db_connections.get_fungrosencrantz_schema('kickstarter')

    '''
    get_descriptive_stats_latex(db=db, table="""
    (select amount_raised, donation_count, IFNULL(team_members, 1) as team_members from (
SELECT
  fundraiser.url,
  fundraiser.total_raised   AS amount_raised,
  IFNULL(donation_count, 0) AS donation_count
FROM fundraiser
  LEFT JOIN (SELECT
               url,
               count(*) AS donation_count
             FROM donation
             GROUP BY url) AS t1
    ON fundraiser.url = t1.url) as t2

left join (select fundraiser_url, count(*) as team_members from team group by fundraiser_url) as t3
  on t2.url = t3.fundraiser_url) as t4""",
                                manual=True, outfile_name='crowdrise_fundraiser',
                                variables=['amount_raised', 'donation_count', 'team_members'])
    '''

    # get_descriptive_stats_latex(db=db, table='team',
    #                            manual=True, outfile_name='crowdrise_team',
    #                            variables=['amount_raised', 'goal'])

    get_descriptive_stats_latex(db, table='project', outfile_name='kickstarter', variables=[
        # 'goal',
        'datediff(end_date, start_date)',
        # 'amount_pledged',
        'backer_count',
        'comment_count',
        'has_video',
        'body_length'
    ],
                                currency_variables=[
                                    dict(
                                        variable='goal',
                                        currency_column='currency',
                                        date_column='date(start_date)'
                                    ),
                                    dict(
                                        variable='amount_pledged',
                                        currency_column='currency',
                                        date_column='date(start_date)'
                                    )
                                ]
                                )
    '''

    #get_descriptive_stats_latex(db, table="""(SELECT loan.funded_amount, loan.paid_amount, loan_amount, count(*) as lender_count FROM Kiva.loan join Kiva.loan_lender on loan.id = loan_lender.loan_id group by loan.id) as t1""",
    #                            variables=['funded_amount', 'paid_amount', 'lender_count', 'loan_amount'], manual=True)

    get_descriptive_stats_latex(db, table="""Kiva.loan""", outfile_name='kiva',
                                variables=['funded_amount', 'paid_amount', 'lender_count', 'loan_amount'], manual=True)
    '''
'''
    q = """
SELECT
    *
FROM
    (SELECT
        'url',
            'FIPS',
            'category',
            'founder_name',
            'goal',
            'has_same_location',
            'has_same_name',
            'has_video',
            'location',
            'month',
            'num_audio',
            'num_backed',
            'num_created',
            'num_friends',
            'num_img',
            'num_media',
            'num_perks',
            'num_video',
            'num_websites',
            'ratio_raised',
            'state',
            'subcategory',
            'success',
            'title',
            'verified_name',
            'year',
            'amount_raised',
            'location_name',
            'location_type',
            'currency',
            'description'
     UNION SELECT
        IFNULL(url, 'none') url,
            IFNULL(NULL, 'none') AS `FIPS`,
            IFNULL(category, 'none') category,
            IFNULL(founder_name, 'none') founder_name,
            IFNULL(goal, 'none') goal,
            IFNULL(NULL, 'none') AS `has_same_location`,
            IFNULL(NULL, 'none') AS `has_same_name`,
            IFNULL(has_video, 'none') has_video,
            IFNULL(raw_location, 'none') AS location,
            IFNULL(MONTH(start_date), 'none') AS `month`,
            IFNULL(NULL, 'none') AS `num_audio`,
            IFNULL(NULL, 'none') AS `num_backed`,
            IFNULL(NULL, 'none') AS `num_created`,
            IFNULL(NULL, 'none') AS `num_friends`,
            IFNULL(body_image_count, 'none') AS `num_img`,
            IFNULL(NULL, 'none') AS `num_media`,
            IFNULL(num_rewards, 'none') AS `num_perks`,
            IFNULL(body_video_count, 'none') AS `num_video`,
            IFNULL(NULL, 'none') AS `num_websites`,
            IFNULL(amount_pledged / goal, 'none') AS ratio_raised,
            IFNULL(location_state, 'none') state,
            IFNULL(subcategory, 'none') subcategory,
            IFNULL(`status` = 'successful', 'none') AS success,
            IFNULL(TRIM(REPLACE(REPLACE(REPLACE(title, '\r', ' '), '\n', ' '), '\t', ' ')), 'none') title,
            IFNULL(NULL, 'none') AS verified_name,
            IFNULL(YEAR(start_date), 'none') AS `year`,
            IFNULL(amount_pledged, 'none') AS amount_raised,
            IFNULL(location_name, 'none') AS location_name,
            IFNULL(location_type, 'none') AS location_type,
            IFNULL(currency, 'none') AS currency,
            IFNULL(TRIM(REPLACE(REPLACE(REPLACE(description, '\r', ' '), '\n', ' '), '\t', ' ')), 'none') as description
    FROM
        (SELECT
        project.*,
            location.`name` AS location_name,
            location.`type` AS location_type,
            location.`state` AS location_state
    FROM
        project
    JOIN location ON project.location_slug = location.slug
    WHERE
        YEAR(start_date) < 2016
            AND location.country = 'US') AS t1
    LEFT JOIN (SELECT
        id AS project_id, COUNT(*) AS num_rewards
    FROM
        project
    JOIN reward ON project.id = reward.projectid
    GROUP BY project.id) AS t2 ON t1.id = t2.project_id) AS t3
;"""

    import pandas as pd
    file_name = 'kickstarter_dump'
    writer = pd.ExcelWriter(file_name + '.xlsx', engine='xlsxwriter', options={'strings_to_urls': False})
    print 'reading sql'
    df = pd.read_sql(sql=q, con=db.executable, index_col='url')
    print 'done reading'
    df.to_excel(writer)
    writer.save()
'''
