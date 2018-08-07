import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import db_connections

db1 = db_connections.get_fungrosencrantz_schema('TestDBforScott')
db2 = db_connections.get_fungrosencrantz_schema('Kiva')

for table in db1.tables:
    original_count = db1.query('select count(*) as cnt from {}'.format(table)).next()['cnt']
    new_count = db2.query('select count(*) as cnt from {}'.format(table)).next()['cnt']

    if original_count != new_count:
        print(table, original_count, new_count)
