import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')

from unused_scripts import db_connections
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.dates as mdates
import datetime
from utils import useful_functions
import matplotlib.ticker as mticker
from matplotlib.colors import LogNorm


def plot_quarterly_df(fig, df, filename, xlabel, ylabel, title, x_series, y_series):
    color_dict = dict()
    color_dict[1] = '#C2AFF0'
    color_dict[2] = '#9191E9'
    color_dict[3] = '#457EAC'
    color_dict[4] = '#2D5D7B'

    fig.clear()
    ax = fig.add_axes((.18, .2, .80, .75))
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    print
    df

    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())
    colors = np.vectorize(color_dict.get)((np.array(range(len(df)))) % 4 + 1)

    ax.bar(range(len(y_series)), y_series, width=1, tick_label=x_series, color=colors)
    ax.set_xticks(np.array(range(len(df))) + 1 / 2.)
    ax.get_yaxis().set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    plt.xticks(rotation='vertical')
    plt.title(title)
    plt.savefig(filename, dpi=350)


def plot_quarterly_stacked_df(fig, df, filename, xlabel, ylabel, title, x_series, y_series, y_series_2, y_series_labels):
    # color_dict = dict()
    # color_dict[1] = '#C2AFF0'
    # color_dict[2] = '#9191E9'
    # color_dict[3] = '#457EAC'
    # color_dict[4] = '#2D5D7B'

    fig.clear()
    ax = fig.add_axes((.18, .2, .80, .75))
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    print
    df

    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())

    p1 = ax.bar(range(len(y_series)), y_series, width=.8, tick_label=x_series, color='#C2AFF0')
    p2 = ax.bar(range(len(y_series)), y_series_2, width=.8, tick_label=x_series, color='#457EAC', bottom=y_series)
    ax.legend([p1, p2], y_series_labels, loc='best')
    ax.set_xticks(np.array(range(len(df))) + 1 / 2.)
    ax.get_yaxis().set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    plt.xticks(rotation='vertical')
    plt.title(title)
    plt.savefig(filename, dpi=350)


def plot_num_projects_per_day(db, fig, file_name_prepend, category_requirement, category):
    fig.clear()
    years = mdates.YearLocator()  # every year
    months = mdates.MonthLocator()  # every month
    yearsFmt = mdates.DateFormatter('%Y')

    query = """
    select count(*) as `count`, start_date as `start_date` from project
    JOIN location ON project.location_slug = location.slug
      WHERE location.country = 'US'
          AND start_date is not null and category {}
       AND NOT (year(start_date) = year(now()) and quarter(start_date) = quarter(now()))
    group by start_date ASC;
    """.format(category_requirement)
    df = pd.read_sql(query, db.engine, parse_dates=['start_date'])

    print
    df

    ax = fig.add_subplot(111)
    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(yearsFmt)
    ax.xaxis.set_minor_locator(months)
    datemin = datetime.date(df['start_date'].min().year, 1, 1)
    datemax = datetime.date(df['start_date'].max().year + 1, 1, 1)
    ax.set_xlim(datemin, datemax)
    ax.set_ylabel('Number of Projects')
    ax.set_xlabel('Start Date')
    ax.plot(df['start_date'], df['count'])
    plt.title('Number of Projects Started Each Day{}'.format(
        '' if category is None else " ({})  ".format(category)))
    plt.savefig(file_name_prepend + 'projects_total_daily.png', dpi=350)


def output_graphs(category=None, category_in_title=None):
    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    fig = plt.figure(figsize=(8, 6))
    if category is None:
        file_name_prepend = 'plot_all_'
        category_requirement = ' is not null '
    else:
        file_name_prepend = 'plot_{}_'.format(category.replace(' ', '').replace('&', '_and_'))
        category_requirement = ' = "{}"'.format(category)

    query = """
        SELECT
          concat(year(start_date), 'Q', quarter(start_date)) AS `quarter`,
          sum(new_backers)                                   AS `new_backers`,
          sum(repeat_backers)                                AS `repeat_backers`,
          std(new_backers)                                   AS `new_backers_std`,
          std(repeat_backers)                                AS `repeat_backers_std`
        FROM project JOIN location ON project.location_slug = location.slug
        WHERE location.country = 'US'
          AND new_backers IS NOT NULL AND project.repeat_backers IS NOT NULL
          AND NOT (year(start_date) = year(now()) AND quarter(start_date) = quarter(now()))
          AND category {}
        GROUP BY year(start_date), quarter(start_date);
    """.format(category_requirement)
    df = pd.read_sql(query, db.engine)
    plot_quarterly_stacked_df(fig=fig,
                              df=df,
                              filename=file_name_prepend + 'backers_split.png',
                              xlabel='Starting Quarter',
                              ylabel='Number of Backers',
                              title='Number of Backers by Quarter{}'.format(
                                  '' if category_in_title is None else " ({})  ".format(category_in_title)),
                              x_series=df['quarter'],
                              y_series=df['repeat_backers'],
                              y_series_2=df['new_backers'],
                              y_series_labels=['Repeat Backers', 'New Backers'])

    plot_num_projects_per_day(db=db, fig=fig,
                              file_name_prepend=file_name_prepend,
                              category_requirement=category_requirement,
                              category=category_in_title, )

    query = """
    select amount_pledged, currency, start_date, concat(year(start_date), 'Q', quarter(start_date)) as `quarter`
    from project JOIN location ON project.location_slug = location.slug
    where location.country = 'US'
          AND start_date is not null and amount_pledged is not null and category {}
       AND NOT (year(start_date) = year(now()) and quarter(start_date) = quarter(now()))
    order by rand();
    """.format(category_requirement)

    pledged_df = pd.read_sql(query, db.engine, parse_dates=['start_date'])
    useful_functions.convert_currency(
        pledged_df, currency_column='currency', money_column='amount_pledged', date_column='start_date')
    del pledged_df['currency']
    del pledged_df['start_date']

    df = pledged_df.groupby(by=['quarter']).sum()
    plot_quarterly_df(fig=fig,
                      df=df,
                      filename=file_name_prepend + 'pledged_total.png',
                      xlabel='Starting Quarter',
                      ylabel='Amount Pledged (USD)',
                      title='Total Amount Pledged by Quarter{}'.format(
                          '' if category_in_title is None else " ({})  ".format(category_in_title)),
                      x_series=df.index,
                      y_series=df['amount_pledged'])

    df = pledged_df.groupby(by=['quarter']).mean()
    plot_quarterly_df(fig=fig,
                      df=df,
                      filename=file_name_prepend + 'pledged_avg.png',
                      xlabel='Starting Quarter',
                      ylabel='Amount Pledged (USD)',
                      title='Average Amount Pledged per Project by Quarter{}'.format(
                          '' if category_in_title is None else " ({})  ".format(category_in_title)),
                      x_series=df.index,
                      y_series=df['amount_pledged'])

    query = """
    SELECT
      concat(year(start_date), 'Q', quarter(start_date)) AS `quarter`,
      avg(backer_count)                                  AS `mean_backers`,
      sum(backer_count)                                  AS `total_backers`
    FROM project JOIN location ON project.location_slug = location.slug
    WHERE location.country = 'US'
          AND start_date IS NOT NULL and category {}
       AND NOT (year(start_date) = year(now()) and quarter(start_date) = quarter(now()))
    GROUP BY year(start_date), quarter(start_date)
    """.format(category_requirement)

    df = pd.read_sql(query, db.engine)
    plot_quarterly_df(fig=fig,
                      df=df,
                      filename=file_name_prepend + 'backers_avg.png',
                      xlabel='Starting Quarter',
                      ylabel='Number of Backers',
                      title='Average Number of Backers per Project by Quarter{}'.format(
                          '' if category_in_title is None else " ({})  ".format(category_in_title)),
                      x_series=df['quarter'],
                      y_series=df['mean_backers'])

    plot_quarterly_df(fig=fig,
                      df=df,
                      filename=file_name_prepend + 'backers_total.png',
                      xlabel='Starting Quarter',
                      ylabel='Number of Backers',
                      title='Total Number of Backers by Quarter{}'.format(
                          '' if category_in_title is None else " ({})  ".format(category_in_title)),
                      x_series=df['quarter'],
                      y_series=df['total_backers'])

    query = """
    SELECT
      concat(year(start_date), 'Q', quarter(start_date)) AS `quarter`,
      count(*) `count`
    FROM project JOIN location ON project.location_slug = location.slug
    WHERE location.country = 'US'
          AND start_date is not null and category {}
       AND NOT (year(start_date) = year(now()) and quarter(start_date) = quarter(now()))
    GROUP BY year(start_date), quarter(start_date)
    """.format(category_requirement)
    df = pd.read_sql(query, db.engine)
    plot_quarterly_df(fig=fig,
                      df=df,
                      filename=file_name_prepend + 'projects_total_quarterly.png',
                      xlabel='Starting Quarter',
                      ylabel='Number of Projects',
                      title='Number of Projects Started Each Quarter{}'.format(
                          '' if category_in_title is None else " ({})  ".format(category_in_title)),
                      x_series=df['quarter'],
                      y_series=df['count'])


def download_plot_data():
    db = db_connections.get_fungrosencrantz_schema('kickstarter')

    query = """
        SELECT
          concat(year(start_date), 'Q', quarter(start_date)) AS `quarter`,
          backer_count, new_backers, repeat_backers, amount_pledged, currency,
          start_date, category, status, goal, founder_id
        FROM project
          JOIN location ON project.location_slug = location.slug
        WHERE location.country = 'US'
          AND NOT (year(start_date) = year(now()) AND quarter(start_date) = quarter(now()))
        order by rand(1)
    """
    full_df = pd.read_sql(query, db.engine, parse_dates=['start_date'])
    useful_functions.convert_currency(
        full_df, currency_column='currency', money_column='amount_pledged', date_column='start_date')
    del full_df['currency']
    del full_df['start_date']

    full_df.to_pickle('plot_data.pickle')


def plot_boxplot(fig, data, x_labels, title, filename, x_label, y_label):
    locations = np.arange(len(x_labels)) + 1
    fig.clear()
    ax = fig.add_axes((.18, .2, .80, .75))
    # ax = fig.add_axes((.2, .2, .70, .6))
    plot = ax.boxplot(data,
                      widths=0.7,
                      # notch=True,
                      positions=locations,
                      patch_artist=True,
                      showfliers=False,
                      showmeans=True,
                      whis=[0, 90]
                      )

    ax.yaxis.set_minor_locator(mticker.AutoMinorLocator())
    ax.set_ylim(0, ax.get_ylim()[1])
    plt.setp(plot['whiskers'], color='DarkMagenta', linewidth=1.5)
    plt.setp(plot['caps'], color='DarkMagenta', linewidth=1.5)
    plt.setp(plot['fliers'], color='OrangeRed', marker='o', markersize=10)
    plt.setp(plot['medians'], color='OrangeRed', linewidth=1.5)
    ax.get_yaxis().set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    plt.xticks(locations,  # tick marks
               x_labels,  # labels
               rotation='vertical')  # rotate the labels
    plt.title(title)
    fig.text(.2, .02, 'Whiskers are at the 0th and 90th percentiles.')
    plt.savefig(filename, dpi=350)
    # plt.show()


def plot_boxplot_from_grouped_df(grouped_df, column_name, fig, x_labels, title, filename, x_label, y_label):
    data = []
    for quarter in x_labels:
        df = grouped_df.get_group(quarter)
        data.append(
            df[column_name][df[column_name].notnull()].values)
    plot_boxplot(fig=fig, data=data, x_labels=x_labels,
                 title=title, filename=filename, x_label=x_label, y_label=y_label)


def run(category=None, category_in_title=None):
    full_df = pd.read_pickle('plot_data.pickle')
    if category is not None:
        full_df = full_df.loc[full_df['category'] == category]
        file_name_prepend = 'plot_{}_'.format(category.replace(' ', '').replace('&', '_and_'))
    else:
        file_name_prepend = 'plot_all_'

    grouped_df = full_df.groupby('quarter')
    fig = plt.figure(figsize=(8, 6))
    quarters = grouped_df.groups.keys()
    quarters.sort()

    plot_boxplot_from_grouped_df(grouped_df=grouped_df, fig=fig, x_labels=quarters,
                                 column_name='repeat_backers',
                                 title='Repeat Backers per Project by Quarter{}'.format(
                                     '' if category_in_title is None else " ({})  ".format(
                                         category_in_title)),
                                 filename=file_name_prepend + 'backers_repeat_box.png',
                                 x_label='Starting Quarter',
                                 y_label='Repeat Backers')
    plot_boxplot_from_grouped_df(grouped_df=grouped_df, fig=fig, x_labels=quarters,
                                 column_name='new_backers',
                                 title='New Backers per Project by Quarter{}'.format(
                                     '' if category_in_title is None else " ({})  ".format(
                                         category_in_title)),
                                 filename=file_name_prepend + 'backers_new_box.png',
                                 x_label='Starting Quarter',
                                 y_label='New Backers')
    plot_boxplot_from_grouped_df(grouped_df=grouped_df, fig=fig, x_labels=quarters,
                                 column_name='backer_count',
                                 title='Backers per Project by Quarter{}'.format(
                                     '' if category_in_title is None else " ({})  ".format(
                                         category_in_title)),
                                 filename=file_name_prepend + 'backers_all_box.png',
                                 x_label='Starting Quarter',
                                 y_label='Number of Backers')
    plot_boxplot_from_grouped_df(grouped_df=grouped_df, fig=fig, x_labels=quarters,
                                 column_name='amount_pledged',
                                 title='Amount Pledged (USD) per Project by Quarter{}'.format(
                                     '' if category_in_title is None else " ({})  ".format(
                                         category_in_title)),
                                 filename=file_name_prepend + 'amount_pledged_box.png',
                                 x_label='Starting Quarter',
                                 y_label='Amount Pledged (USD)')


def output_founder_graphs():
    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    fig = plt.figure(figsize=(8, 6))

    query = """
        SELECT
          collapsed_projects_by_founder                  AS `num_projects_by_founder`,
          sum(num_successful) `num_successful`,
          sum(num_failed) `num_failed`,
          sum(num_other) `num_other`,
          sum(num_successful)/sum(projects_by_founder) `success_rate`,
          count(*) `num_founders`
        FROM (
               SELECT
                 founder_id,
                 count(*)                            AS projects_by_founder,
                 #IF(count(*) <= 10, count(*), 11) AS collapsed_projects_by_founder,
                 count(*) AS collapsed_projects_by_founder,
                 sum(status = 'successful')          AS num_successful,
                 sum(status = 'failed') `num_failed`,
                 sum(status not in ('successful', 'failed')) `num_other`
               FROM project
                 JOIN location ON project.location_slug = location.slug
               WHERE location.country = 'US' AND start_date < (SELECT min(start_date)
                                FROM project
                                WHERE status = 'live')
               GROUP BY founder_id) AS t1
        GROUP BY collapsed_projects_by_founder;
    """
    df = pd.read_sql(query, db.engine)

    y = df['num_founders'].values
    x = df['num_projects_by_founder'].values
    # size = np.sqrt(df['num_founders'].values * 200)

    ax = fig.add_axes((.18, .2, .80, .75))
    plot = ax.scatter(x, y, marker='o', c=df['success_rate'], cmap='gray_r')
    fig.colorbar(plot, label='Success Rate')

    plt.title('Number of Projects Made by a Founder vs Number of Founders')
    plt.xlabel('Number of Projects Made by Founder')
    plt.ylabel('Number of Founders')

    ax.set_yscale('log')

    plt.savefig('plot_founder_distribution.png', dpi=350)


def output_goal_vs_funded_graph():
    df = pd.read_pickle('plot_data.pickle')

    """
    side = np.linspace(-2, 2, 15)
    X, Y = np.meshgrid(side, side)
    Z = np.exp(-((X - 1) ** 2 + Y ** 2))

    # Plot the density map using nearest-neighbor interpolation
    plt.pcolormesh(X, Y, Z)
    """
    # Generate some test data
    # x = np.random.randn(8873)
    # y = np.random.randn(8873)
    # x = np.log10(df['goal'][df['goal'].notnull()][df['amount_pledged'].notnull()])
    # y = np.log10(df['amount_pledged'][df['goal'].notnull()][df['amount_pledged'].notnull()])
    # df = df[df['status'] == 'failed']
    x = df['goal'][df['goal'].notnull()][df['amount_pledged'].notnull()] + 1
    y = df['amount_pledged'][df['goal'].notnull()][df['amount_pledged'].notnull()].values + 1

    # x, y = np.random.random((2, 1000))
    # x = 10 ** x

    # xbins = np.linspace(0, 100000, 50)
    # ybins = np.linspace(0, 100000, 50)
    axis = 10 ** np.linspace(0, 8, 41)

    counts, _, _ = np.histogram2d(x, y, bins=(axis, axis), normed=False)

    fig, ax = plt.subplots()
    heatmap = ax.pcolormesh(axis, axis, counts, norm=LogNorm(), alpha=1)
    cbar = plt.colorbar(heatmap)
    cbar.ax.set_ylabel('# of projects')

    # ax.plot(x, y, 'ro')

    ax.set_xscale('log')
    ax.set_yscale('log')

    ax.set_ylabel('Amount Pledged (USD)')
    ax.set_xlabel('Goal (USD)')
    plt.title('Goal vs Amount Pledged')

    plt.plot([10 ** 0, 10 ** 8], [10 ** 0, 10 ** 8], color='black')

    plt.savefig('plot_goal_vs_funded.png', dpi=350)


if __name__ == '__main__':
    # output_graphs()
    # output_graphs('technology', "Technology")
    # download_plot_data()
    # run('technology', "Technology")
    # run()
    output_founder_graphs()
    # output_goal_vs_funded_graph()
