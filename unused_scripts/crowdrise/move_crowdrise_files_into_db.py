import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import db_connections
import logging
import traceback
import time
import os
from useful_functions import split_array_into_chunks


def run():
    db = db_connections.get_fungrosencrantz_schema('crowdrise')
    chunk_size = 100

    html_data = []
    for chunk_index, chunk in enumerate(split_array_into_chunks(
            [x['file_name'] for x in db.query('''select file_name from all_files left join html on all_files.file_name = html.url
where html.url is null;''')],
            chunk_size=chunk_size)):
        t0 = time.time()
        for file_index, file_path in enumerate(chunk):
            cur_index = chunk_index * chunk_size + file_index
            try:
                with open(file_path, 'rb') as f:
                    f_read = f.read()
            except IOError:
                logging.error(traceback.format_exc())
                try:
                    with open(file_path.replace('E:\\', ''), 'rb') as f:
                        f_read = f.read()
                except IOError:
                    with open('files_not_found', 'a') as f:
                        f.write(file_path + '\n')
                    continue
            # logging.debug('{}: {}'.format(cur_index, file_path))
            html_data.append(dict(
                url=file_path,
                html=f_read,
                last_scrape=time.gmtime(os.path.getmtime(file_path)),
            ))
        db['html'].insert_many(html_data, ensure=False)
        logging.debug('inserted through {}; took {}'.format(cur_index, time.time() - t0))
        html_data = []

    # for table, data in crowdrise_data.items():
    #    db[table].drop()
    #    db[table].insert_many(data)
    # db['user'].drop()
    # db['user'].insert_many(crowdrise_data['user'])
    # db['charity'].drop()
    # db['charity'].insert_many(crowdrise_data['charity'])
    # db_connections.uploadOutputFile(data=crowdrise_data['fundraiser'], db=db, table='fundraiser')


def select():
    db = db_connections.get_fungrosencrantz_schema('crowdrise')
    t0 = time.time()
    q = [x for x in db.query('select * from html limit 300')]
    logging.debug('reading {} files took {} seconds'.format(len(q), time.time() - t0))


if __name__ == '__main__':
    t0 = time.time()
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG)  # , filename=log_file)  #
    logging.debug("Starting {}.".format(sys.argv[0]))
    try:
        # for table in ('fundraiser', 'special_user', 'user', 'front_page_redirect'):
        #    run(table)
        run()
        # select()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        raise
    logging.info('{} has completed in {} seconds.'.format(sys.argv[0], time.time() - t0))
