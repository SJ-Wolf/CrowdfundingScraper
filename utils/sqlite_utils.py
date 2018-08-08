import sqlite3
import uuid

from utils.file_utils import contextmanager


def get_tmp_table_name():
    return '__tmp_' + str(uuid.uuid4())


@contextmanager
def tmp_table(df, conn):
    name = get_tmp_table_name()
    df.to_sql(name, conn, index=False)
    try:
        yield name
    finally:
        cur = conn.cursor()
        cur.execute(f'drop table if exists "{name}"')


def insert_into_table(df, table_name, conn, replace=False):
    if len(df) == 0:
        return
    cur = conn.cursor()
    tmp_table_name = '__tmp_' + table_name + '_' + str(uuid.uuid4())
    cur.execute(f'create temporary table "{tmp_table_name}" as select * from "{table_name}" where 0;')
    df.to_sql(tmp_table_name, conn, if_exists='append', index=False)
    cur.execute(('replace' if replace else 'insert or ignore') + f' into "{table_name}" select * from "{tmp_table_name}";')


def delete_temporary_tables(database_location: str):
    with sqlite3.connect(database_location) as db:
        cur = db.cursor()
        cur.execute(r"""
            select tbl_name
            from sqlite_master
            where type = 'table' and tbl_name like '\_\_tmp\_%' escape '\'
                                                    """)
        table_names = [x[0] for x in cur.fetchall()]
        for table in table_names:
            cur.execute(f'drop table if exists `{table}`')


def get_create_table_statements(database_location: str):
    with sqlite3.connect(database_location) as db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute('select sql from sqlite_master where name not like "__tmp_%" and name not like "sqlite_autoindex_%" and type = "table"')
        statements = [row['sql'] + ';' for row in cur.fetchall()]
        cur.execute('select sql from sqlite_master where name not like "__tmp_%" and name not like "sqlite_autoindex_%" and type != "table"')
        statements += [row['sql'] + ';' for row in cur.fetchall()]
        return statements
