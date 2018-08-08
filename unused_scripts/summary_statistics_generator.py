""" creates summary statistics for the main tables in the Kickstarter database.
outputs to {table name}.xlsx"""

import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import time
from unused_scripts import db_connections
import tablib

# set this to false if this script is one folder lower than the main folder (eg if it's in unused_scripts)
IS_TOP_LEVEL = False


def generate_simple_function_query(table, columns, function="avg"):
    q = "select "
    for i, col in enumerate(columns):
        if i != 0:
            q += ", "
        q += "{0}({1})".format(function, col)
    q += " from {0}".format(table)
    return q


def generate_median_query(table, columns):
    q = "select * from\n"
    for i, col in enumerate(columns):
        if i != 0:
            q += ", "
        q += """(
	SELECT avg(t1.{0}) as median_val FROM (
	SELECT @rownum:=@rownum+1 as `row_number`, d.{0}
	  FROM {1} d,  (SELECT @rownum:=0) r
	  WHERE 1
	  -- put some where clause here
	  ORDER BY d.{0}
	) as t1,
	(
	  SELECT count(*) as total_rows
	  FROM {1} d
	  WHERE 1
	  -- put same where clause here
	) as t2
	WHERE 1
	AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) )
) as t{2}""".format(col, table, i)
    return q


def generate_query(table, column, round_digits=4):
    headers = ['N', 'Mean', 'SD', 'Median', 'Min', 'Max']
    q = """select
	count({0}) as {2[0]},
    round(avg({0}),{3}) as {2[1]},
	round(std({0}),{3}) as {2[2]},
        (
    SELECT avg(t1.{0}) as median_val FROM
		(
			SELECT @rownum:=@rownum+1 as `row_number`, d.{0}
			  FROM {1} d,  (SELECT @rownum:=0) r
			  WHERE {0} is not null
			  -- put some where clause here
			  ORDER BY d.{0}
			) as t1,
			(
			  SELECT count(*) as total_rows
			  FROM {1} d
			  WHERE {0} is not null
			  -- put same where clause here
			) as t2
			WHERE 1
			AND t1.row_number in ( floor((total_rows+1)/2), floor((total_rows+2)/2) )
		) as {2[3]},
    min({0}) as {2[4]},
    max({0}) as {2[5]}
from {1} where {0} is not null""".format(column, table, headers, round_digits)
    return q, headers


'''
# returns the query to run, row headers
def generate_full_statistics_query(table, columns):
    q = generate_simple_function_query(table, columns, 'count')
    q += "\nunion\n"
    q += generate_simple_function_query(table, columns, 'avg')
    q += "\nunion\n"
    q += generate_median_query(table, columns)
    q += "\nunion\n"
    q += generate_simple_function_query(table, columns, 'std')
    q += "\nunion\n"
    q += generate_simple_function_query(table, columns, 'min')
    q += "\nunion\n"
    q += generate_simple_function_query(table, columns, 'max')
    return q, ['N', 'Avg', 'Std', 'Median', 'Min', 'Max']
'''


def generate_full_statistics_query(table, columns):
    q = ""
    for i, c in enumerate(columns):
        if i != 0:
            q += "\nunion\n"
        sub_q, headers = generate_query(table, c)
        q += sub_q
    return q, headers


def generate_statistics_file(table, columns, table_columns=None):
    db = db_connections.get_fungrosencrantz_schema(traditional=True)
    for c in columns:
        if table_columns is None:
            assert c in db[table].columns
        else:
            assert c in table_columns
    q, headers = generate_full_statistics_query(table=table, columns=columns)

    data = []
    try:
        for i, row in enumerate(db.query(q)):
            row['Name'] = columns[i]
            data.append(row)
    except:
        print(q)
        time.sleep(0.2)
        raise
    tablib_data_unordered = tablib.Dataset()
    tablib_data_unordered.dict = data

    tablib_data = tablib.Dataset()
    for header in ['Name'] + headers:
        tablib_data.append_col(tablib_data_unordered[header], header=header)

    tablib_data.headers = ['Name'] + headers
    with open('{0}_statistics.xlsx'.format(table), 'wb') as f:
        f.write(tablib_data.xlsx)


project_columns = ['id', 'goal', 'amount_pledged', 'start_date', 'end_date', 'duration', 'backer_count', 'comment_count', 'update_count',
                   'body_length', 'body_image_count', 'body_video_count', 'has_video']
project_table = 'project_duration'

reward_columns = ['projectid', 'reward_number', 'amount_required', 'backer_limit', 'backer_count']
reward_table = 'reward'

comments_columns = ['id', 'by_creator', 'post_date']
comments_table = 'comments'

update_columns = ['projectid', 'update_number', 'post_date']
update_table = '`update`'

location_columns = ['id', 'is_root']
location_table = 'location'

funding_trend_columns = ['projectid', 'date_added', 'amount_pledged', 'backer_count', 'comment_count', 'update_count']
funding_trend_table = 'funding_trend'

# if not IS_TOP_LEVEL:
#    os.chdir('..')

t1 = time.time()
# generate_statistics_file(project_table, project_columns)
# generate_statistics_file(reward_table, reward_columns)
# generate_statistics_file(comments_table, comments_columns)
# generate_statistics_file(update_table, update_columns, table_columns=update_columns)
# generate_statistics_file(location_table, location_columns)
# generate_statistics_file(funding_trend_table, funding_trend_columns)
print("Generation took {} seconds.".format(time.time() - t1))
