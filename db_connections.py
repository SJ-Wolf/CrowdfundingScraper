import psycopg2
import sys
import dataset
import json
from sqlalchemy.exc import OperationalError
import time
import logging

verbose_level = 1


class IdMaker:
    def __init__(self):
        self.id = 0

    def get_id(self):
        self.id += 1
        return self.id


id_maker = IdMaker()


# similar to uploadOutputFile, except data is now a dictionary - the keys being the tables to upload to and their
# values are sent to uploadOutputFile
def multi_table_upload(data, db, strict=True, update=True, chunk_size=10000, ensure=False, ordered_tables=[], process_num=None):
    for table in ordered_tables:
        logging.info('Uploading data to {}'.format(table))
        uploadOutputFile(data[table], db=db, table=table, strict=strict, update=update, chunk_size=chunk_size,
                         ensure=ensure, process_num=process_num)
    for table in set(data.keys()) - set(ordered_tables):
        logging.info('Uploading data to {}'.format(table))
        uploadOutputFile(data[table], db=db, table=table, strict=strict, update=update, chunk_size=chunk_size,
                         ensure=ensure, process_num=process_num)


# copies the contents of data into table, updating on duplicates
def uploadOutputFile(data=[dict()], db=dataset.connect(), table="project", strict=True, update=True, chunk_size=10000,
                     ensure=True, process_num=None):
    if data is None or len(data) == 0:
        return
    if len(data) > chunk_size:
        cur_index = 0
        while cur_index < len(data):
            # db.engine.connect()
            logging.debug("About to call uploadOutputFile, db={} , table={}".format(db, table))
            uploadOutputFile(data[cur_index:cur_index + chunk_size], db=db, table=table, strict=strict, update=update,
                             chunk_size=chunk_size, ensure=ensure, process_num=process_num)
            cur_index += chunk_size
    else:
        columns = db[table].columns
        if ensure:
            logging.debug('Ensuring columns exist in data')
            for column in columns:
                for row_index in range(len(data)):
                    cur_value = data[row_index].get(column)
                    if cur_value is None or cur_value == '':
                        data[row_index][column] = None
        db.begin()
        logging.debug("db.tables = {}".format(db.tables))
        if process_num is None:
            tmp_table = "tmp_{0}".format(id_maker.get_id())
        else:
            tmp_table = "tmp_{}_{}".format(process_num, id_maker.get_id())
        logging.debug("tmp_table = {}".format(tmp_table))
        assert tmp_table not in db.tables
        assert table in db.tables
        # sqlite cannot be set to TRADITIONAL mode
        if strict:
            try:
                db.query("SET SESSION sql_mode = 'TRADITIONAL'")
            except OperationalError:
                raise Exception("Sqlite3 cannot be in strict mode (TRADITIONAL)")

        # create tmp_table like table that will be copied to
        table_columns_repr = repr(columns).replace("'", "`").replace("[", "").replace("]", "")
        """
        if db.engine.name == 'sqlite':
            q = db.query('SELECT sql FROM sqlite_master WHERE type="table" AND name="{0}"'.format(table)).next()['sql']
            q_split = q.split(" ")
            q_split[2] = tmp_table
            q = " ".join(q_split)
            db.query(q)
            print'\n\nDEBUG: selecting from sqlite_master table '
        else:
            q = "create temporary table `{0}` like `{1}`".format(tmp_table, table)
            db.query(q)
            if verbose_level > 1: print'\n\n ***  DEBUG: Creating table --->', q
            if verbose_level > 1: print'\n\n ***  DEBUG: db.engine.name --->', db.engine.name"""
        try:
            """
            if db.engine.name == 'sqlite':
                if verbose_level > 5: print'DEBUG:  tmp_table_columns =', db[tmp_table].columns
                tmp_table_columns = set(db[tmp_table].columns)
                for row in data:
                    #if verbose_level > 5: print'DEBUG:  row and data =', row, data
                    #if verbose_level > 5: print'DEBUG:  row.keys =', row.keys()
                    if set(row.keys()) != tmp_table_columns:
                        pass
                    assert set(row.keys()) == tmp_table_columns
            db[tmp_table].insert_many(data, ensure=False)"""

            if db.engine.name == 'sqlite':
                db[tmp_table].insert_many(data, ensure=True, chunk_size=chunk_size)
            else:
                q = 'create table `{}` ('.format(tmp_table)
                for i, col in enumerate(columns):
                    q += """`{}` MEDIUMTEXT COLLATE utf8mb4_unicode_ci""".format(col)
                    if i < len(columns) - 1:
                        q += ','
                    q += '\n'
                q += """) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""
                db.query(q)
                db[tmp_table].insert_many(data, ensure=False, chunk_size=chunk_size)

            # copy tmp table to proper table
            logging.debug('Copying tmp table to proper table')
            if db.engine.name == 'sqlite':
                if update:
                    q = """insert or replace into `{0}`
                        select {2} from `{1}`
                        """.format(table, tmp_table, table_columns_repr)
                else:
                    q = """insert or ignore into `{0}`
                        select {2} from `{1}`
                        """.format(table, tmp_table, table_columns_repr)
                logging.debug('insert or replace data table using query:\n{}'.format(q))
                db.query(q)
            else:
                # copy the temporary table into the given table, updating on duplicates
                if update:
                    q = """insert into `{0}`
                            select {2} from `{1}`
                            on duplicate key update""".format(table, tmp_table, table_columns_repr)
                    headers = db[table].columns
                    for i, column in enumerate(headers):
                        if i == 0:
                            q += "\t`{0}`.`{2}` = `{1}`.`{2}`".format(table, tmp_table, column)
                        else:
                            q += ",\n\t`{0}`.`{2}` = `{1}`.`{2}`".format(table, tmp_table, column)
                else:

                    q = """insert ignore into `{0}`
                            select {2} from `{1}`""".format(table, tmp_table, table_columns_repr)
                '''
                headers = db[table].columns
                for i, column in enumerate(headers):
                    if i == 0:
                        q = q + "\t`{0}`.`{2}` = `{1}`.`{2}`".format(table, tmp_table, column)
                    else:
                        q = q + ",\n\t`{0}`.`{2}` = `{1}`.`{2}`".format(table, tmp_table, column)
                if verbose_level > 5: print'DEBUG: insert data table --->',q'''
                retry_num = 0
                while True:
                    try:
                        db.query(q)
                        break
                    except:
                        logging.debug("Error executing the following query:\n{}".format(q))
                        if retry_num > 5:
                            raise
                        else:
                            retry_num += 1
                            time.sleep(1)
        except:
            db.rollback()
            # db[tmp_table].drop()
            raise
            '''try:
                db.query('drop table {}'.format(tmp_table))
            except:
                pass
            raise'''
        db.commit()
        db[tmp_table].drop()
        '''try:
            db.query('drop table {}'.format(tmp_table))
        except:
            pass'''


# finds all `id` from table_name in db where `id` is not in scrape_table_column_name of scrape_table_name
# returns a dictionary where the keys are urls and the values are ids
# query_append is simply appended to the end of the query and could be used as a where or limit
# note that the query sent already ends in a where clause so additional restraints must be added through `and`!
def get_ids_for_base_urls(db, table_name, query_append="", scrape_table_name="", scrape_table_column_name=""):
    q = 'select {0}.url, {0}.id from {0} '.format(table_name)
    if len(scrape_table_name) > 0:
        q += 'left join {1} on {0}.id = {1}.{2} where {1}.{2} is null ' \
            .format(table_name, scrape_table_name, scrape_table_column_name)
    if len(query_append) > 0:
        q += query_append
    result = db.query(q)
    ids_for_base_urls = dict()
    for row in result:
        ids_for_base_urls[row['url']] = row['id']
    return ids_for_base_urls


'''
def load_tsv_file(filename, headers = None):
    data = tablib.Dataset()
    with open(filename, 'rb') as f:
        data.tsv = f.read()
    if headers is not None:
        data.headers = headers
    return data
'''


def split_location_string(inputStr):
    split_str = inputStr.split(", ")
    for i, section in enumerate(split_str):
        if section[-3:] == " of":
            split_str[i - 1] = split_str[i - 1] + ", " + split_str[i]
            del split_str[i]
    return split_str


def get_fungrosencrantz_schema(schema='kickstarter', traditional=False):
    with open("../lib/fungrosencrantz_login", 'r') as f:
        login = json.load(f)
    connections_string = 'mysql+pymysql://' + login['username'] + ":" + login['password'] + "@" + login[
        'hostname'] + "/" + schema + "?charset=utf8mb4"
    for num_tries in range(20):
        try:
            db = dataset.connect(connections_string, row_type=dict,
                                 engine_kwargs={'encoding': 'utf8',
                                                'pool_recycle': 360})
            break
        except:
            logging.warning("Can't connect to database. Error: {}".format(sys.exc_info()))
            raise
            db = None
            time.sleep(90)
    if traditional:
        db.query("SET SESSION sql_mode = 'TRADITIONAL'")
    return db


# database uses unicode
def get_intermediate_db():
    for numTries in range(20):
        try:
            db = dataset.connect('sqlite:///html_database.db', engine_kwargs=dict(pool_recycle=360))
            # db = dataset.connect('sqlite:////home2/crowdfunding-haas/scrape/kickstarter_html.db',
            #                     engine_kwargs=dict(pool_recycle=360))
            break
        except:
            logging.warning("Can't connect to database...")
            db = None
            time.sleep(90)
    return db


def get_theta_postgres_db(dbname='crowdrise'):
    with open('../lib/theta_login') as f:
        login = json.load(f)
    return psycopg2.connect('dbname={} user={} password={} host={}'.format(
        dbname, login['username'], login['password'], login['hostname']
    ))


def repeats(l):
    seen = set()
    seen_add = seen.add
    # adds all elements it doesn't know yet to seen and all other to seen_twice
    seen_twice = set(x for x in l if x in seen or seen_add(x))
    # turn the set into a list (as requested)
    return list(seen_twice)


def json_file_uploader():
    with open('urls_for_ids.json', 'rb') as f:
        urls_for_ids = json.load(f)

    logging.debug(len(urls_for_ids))

    logging.debug("Connecting to kickstarter db")
    db = get_fungrosencrantz_schema('kickstarter', traditional=True)

    existing_ids = [x['id'] for x in db.query('select id from project')]

    logging.debug("Removing existing ids")
    initial_ids = urls_for_ids.keys()
    logging.debug(existing_ids[:100])
    logging.debug(initial_ids[:100])
    existing_ids = set(existing_ids)
    for id in initial_ids:
        urls_for_ids[id] = urls_for_ids[id].split('?')[0]
        if int(id) in existing_ids:
            del urls_for_ids[id]

    int_db = get_intermediate_db()
    logging.debug(len(urls_for_ids))

    data = []
    for id in urls_for_ids:
        data.append({'url': urls_for_ids[id]})
    uploadOutputFile(data, db=int_db, table='urls_to_scrape')

    '''
    with open('locations.json', 'rb') as f:
        locations = json.load(f)
    id_set = set()
    cleaned_locations = []
    for i, loc in enumerate(locations):
        if loc['id'] not in id_set:
            cleaned_locations.append(loc)
            id_set.add(loc['id'])
    #print len(cleaned_locations)
    uploadOutputFile(cleaned_locations, db, table='kickstarter_location_2')
    '''


if __name__ == "__main__":
    """
    headers = ['id', 'description', 'title', 'url', 'goal', 'amount_pledged', 'start_date',
              'end_date', 'status_changed', 'status', 'category', 'raw_location', 'currency',
              'backers_count']
    data = load_tsv_file('C:\\Users\\kyle1\\Google Drive\\2015 Summer Internship\\Databases\\Kickstarter\\outputFull.tsv', headers=headers)

    dataRepeats = repeats(data['id'])
    repeatTracker = dict()
    for repeat in dataRepeats:
        repeatTracker[str(repeat)] = False

    data2 = tablib.Dataset(headers=headers)
    startIndex = 150000
    lastIndex = None
    #data = load_tsv_file('fullOutput.tsv', headers=headers)
    for rowIndex, row in enumerate(data):
        if rowIndex < startIndex: continue
        if lastIndex is not None and rowIndex == lastIndex + 1: break
        if rowIndex % 1000 == 0: print rowIndex

        #id_index = numpy.searchsorted(all_projectids, row[headers.index('id')])
        #if already_in_data2[id_index]: continue
        #done_projects = done_projects.union([row[headers.index('id')]])
        id = row[headers.index('id')]
        if str(id) in repeatTracker.keys():
            if repeatTracker[str(id)] == True:
                continue
            else:
                repeatTracker[str(id)] = True
        dateIndexes = [headers.index('start_date'), headers.index('end_date'), headers.index('status_changed')]
        new_row = list(row)
        for i in dateIndexes:
            if row[i] != "":
                excel_date = row[i].split('/')
                sql_date = excel_date[2] + "-" + excel_date[0] + "-" + excel_date[1]
                new_row[i] = sql_date
        for i, x in enumerate(new_row):
            if x == '': new_row[i] = None
        data2.append(new_row)

    print ""
    print len(data2)
    print len(set(data['id']))
    #with open('fullOutput.tsv', 'wb') as f:
    #    f.write(data2.tsv)
    db = get_fungrosencrantz_schema()
    uploadOutputFile(data2, db=db, strict=True)
    db.engine.close()"""
    pass
    import os

    os.chdir('crowdrise')
    db = get_fungrosencrantz_schema('kickstarter')
    db = get_theta_postgres_db()

    '''sqlalchemy.exc.InternalError: (pymysql.err.InternalError)(1051, u"Unknown table 'TestDBforScott.tmp_1774'")[
                                  SQL: u'\nDROP TABLE tmp_1774']'''
