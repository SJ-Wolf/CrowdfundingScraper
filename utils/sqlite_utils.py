import sqlite3
import uuid
import logging

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


def get_table_columns(table_name, conn):
    prev_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    table_columns = set(
        [x['name'] for x in conn.execute(f'PRAGMA table_info("{table_name}");').fetchall()])
    conn.row_factory = prev_row_factory
    return table_columns


def insert_into_table(df, table_name, conn, replace=False):
    if len(df) == 0:
        return
    restricted_df = df
    table_columns = get_table_columns(table_name, conn)
    df_columns = set(df.columns)
    df_extra_columns = df_columns - table_columns
    table_extra_columns = table_columns - df_columns

    if df_extra_columns:
        logging.warning(f'When inserting into table {table_name}, data had extra columns: {df_extra_columns})')
        good_columns = [x for x in df.columns if x not in df_extra_columns]
        restricted_df = df[good_columns]
    if table_extra_columns:
        logging.info(f'Data has fewer columns than table {table_name}. Missing: {table_extra_columns}')

    df_columns_str = ', '.join(restricted_df.columns)
    question_mark_str = ','.join(['?'] * len(restricted_df.columns))
    conn.executemany(
        ('replace' if replace else 'insert or ignore')
        + f' into "{table_name}" ({df_columns_str}) values ({question_mark_str});',
        restricted_df.values.tolist())


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
        cur.execute(
            'select sql from sqlite_master where name not like "__tmp_%" and name not like "sqlite_autoindex_%" and type = "table"')
        statements = [row['sql'] + ';' for row in cur.fetchall()]
        cur.execute(
            'select sql from sqlite_master where name not like "__tmp_%" and name not like "sqlite_autoindex_%" and type != "table"')
        statements += [row['sql'] + ';' for row in cur.fetchall()]
        return statements
