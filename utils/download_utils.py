import os
import sqlite3
import subprocess
import time
import zipfile
from multiprocessing import Process, Lock, JoinableQueue

import pandas as pd
import requests
from fake_useragent import UserAgent
from lxml import html

from useful_functions import ensure_directory
from utils.file_utils import enter_folder
from utils.sqlite_utils import get_tmp_table_name


def get_urls(urls, per_second=3.0, overwrite=False, max_num_proxies=10, refresh_proxy_list=True, skip_not_existing=False):
    proxies = get_latest_free_proxy_list(refresh=refresh_proxy_list)[:max_num_proxies]
    wait_time_between_requests = max(1.0, len(proxies) / per_second / 2.0)  # at least one second between requests
    print('Wait time between requests:', wait_time_between_requests)
    with sqlite3.connect('html.db') as db:
        cur = db.cursor()
        tmp_table_name = get_tmp_table_name()
        cur.execute(f'create temporary table `{tmp_table_name}` (row_num int, url text);')
        cur.executemany(f'insert into `{tmp_table_name}` values (?, ?)', [(i, url) for i, url in enumerate(urls)])
        if overwrite:
            urls_to_download = urls
        else:
            cur.execute(f'select url from `{tmp_table_name}` as t1 where not exists(select 1 from html where html.url = t1.url)')
            urls_to_download = [x[0] for x in cur.fetchall()]
        db.commit()
        if not skip_not_existing:
            download_urls_through_proxies(proxies, urls_to_download, wait_time_between_requests=wait_time_between_requests,
                                          wait_time_between_adding_urls=1.0 / per_second, to_db=True)

        cur.execute(f'select url, (select page_source from html where html.url = t1.url) from `{tmp_table_name}` as t1 order by row_num')
        while True:
            results = cur.fetchmany(1000)
            if len(results) == 0:
                break
            else:
                for r in results:
                    yield r[1]
        cur.execute('drop table if exists `{tmp_table_name}`')


def wget_urls(urls, overwrite=True, folder='.'):
    with enter_folder(folder):
        with open('urls_to_download', 'w') as f:
            for u in urls:
                f.write(u + '\n')
        if overwrite:
            subprocess.call('wget -N --force-directories -i urls_to_download --output-file=wget_log --no-check-certificate', shell=True)
        else:
            subprocess.call('wget --force-directories -i urls_to_download --output-file=wget_log --no-check-certificate --no-clobber', shell=True)


def get_page_source(url, requests_session=None, wait_time=0, binary=False):
    while True:
        if wait_time > 0:
            time.sleep(wait_time)
        if requests_session is None:
            r = requests.get(url, headers={'User-Agent': UserAgent().chrome})
        else:
            r = requests_session.get(url, headers={'User-Agent': UserAgent().chrome})
        # print(r.headers)
        if r.status_code == 429:
            retry_after = int(r.headers['Retry-After'])
            print(f'Hit limit! Waiting {retry_after} seconds.')
            time.sleep(retry_after)
            continue
        else:
            r.raise_for_status()
            break
    if binary:
        page_source = r.content
    else:
        page_source = r.text
    return page_source


def get_url(url, wait_time=0, requests_session=None, verbose=False, overwrite=False):
    if verbose:
        print(url)
    with sqlite3.connect('html.db') as db:
        cur = db.cursor()
        if overwrite:
            results = []
        else:
            try:
                cur.execute('select page_source from html where url = ?', (url,))
            except sqlite3.OperationalError:
                cur.execute("""
                    create table html
                    (
                        url TEXT
                            primary key,
                        page_source text
                    )
                    ;""")
                cur.execute('select page_source from html where url = ?', (url,))
            results = cur.fetchall()
        if len(results) == 0:
            page_source = get_page_source(url, requests_session=requests_session, wait_time=wait_time)
            cur.execute('replace into html values (?, ?)', (url, page_source))
        else:
            page_source = results[0][0]
        return page_source


def extract_gz_in_folder(folder='downloads'):
    with enter_folder(folder):
        for fname in os.listdir('.'):
            if fname.endswith('.gz'):
                subprocess.call(f'gunzip "{fname}"', shell=True)


def extract_zip_in_folder(folder='downloads', recursive=False):
    with enter_folder(folder):
        if recursive:
            for dir_path, dir_names, file_names in os.walk('.'):
                for f_name in file_names:
                    if f_name.endswith('.zip'):
                        f_name = os.path.join(dir_path, f_name)
                        with zipfile.ZipFile(f_name) as zip_ref:
                            zip_ref.extractall()

        else:
            for f_name in os.listdir('.'):
                if f_name.endswith('.zip'):
                    subprocess.call(f'unzip "{f_name}"', shell=True)


def get_proxy_server_session(proxy_str):
    proxies = {
        'http': proxy_str
    }
    s = requests.session()
    s.proxies = proxies
    r = s.get("http://api.ipify.org/")
    assert proxy_str.split(':')[0] in r.text
    return s


def get_latest_free_proxy_list(refresh=False):
    tree = html.fromstring(get_url('https://free-proxy-list.net/', overwrite=refresh))
    table_elem = tree.xpath('//table[@id="proxylisttable"]')[0]
    headers = table_elem.xpath('./thead/tr/th/text()')

    row_elems = table_elem.xpath('./tbody/tr')

    data = []
    for row_elem in row_elems:
        row = []
        for col in row_elem.xpath('./td/text()'):
            row.append(col)
        assert len(row) == len(headers)
        data.append(row)

    df = pd.DataFrame(data, columns=headers)
    df = df.where((df['Anonymity'] == 'anonymous') | (df['Anonymity'] == 'elite proxy')).dropna()
    return (df['IP Address'] + ':' + df['Port']).values.tolist()


def check_proxy_session(session, ip):
    r = session.get('http://api.ipify.org/')
    return ip in r.text


def download_url_queue_into_db(db_lock, url_queue, proxy_str, wait_time, proc_num, to_db, replace_existing):
    try:
        session = get_proxy_server_session(proxy_str)
    except AssertionError:
        return  # proxy dead on arrival :(
    except Exception as e:
        if 'Cannot connect to proxy' in str(e):
            return
        else:
            raise
    print(proxy_str)
    while True:
        # time.sleep(0.1)
        url = url_queue.get()
        print(proc_num, '\t', url)
        # with db_lock:
        try:
            # get_url(url, requests_session=session, verbose=True, overwrite=True)
            if to_db:
                page_source = get_page_source(url, requests_session=session, wait_time=wait_time, binary=False)
                with db_lock:
                    with sqlite3.connect('html.db') as db:
                        if replace_existing:
                            db.execute('replace into html values (?, ?)', (url, page_source))
                        else:
                            db.execute('insert or ignore into html values (?, ?)', (url, page_source))
            else:
                file_name = url.replace('http://', '').replace('https://', '')
                ensure_directory('/'.join(file_name.split('/')[:-1]))
                if replace_existing or not os.path.exists(file_name):
                    page_source = get_page_source(url, requests_session=session, wait_time=wait_time, binary=True)

                    with open(file_name, 'wb') as f:
                        f.write(page_source)
        except Exception as e:
            print(e)
            if '404' in str(e):
                pass
            else:
                url_queue.put(url)
                break
        url_queue.task_done()


def download_urls_through_proxies(proxy_strings, urls, wait_time_between_requests, wait_time_between_adding_urls=0.3, to_db=True,
                                  replace_existing=True, connections_per_server=1):
    if len(urls) == 0 or len(proxy_strings) == 0 or connections_per_server == 0:
        return None
    url_queue = JoinableQueue(maxsize=0)
    lock = Lock()
    workers = []

    for proc_num, proxy_str in enumerate(proxy_strings):
        for i in range(connections_per_server):
            worker = Process(target=download_url_queue_into_db,
                             args=(lock, url_queue, proxy_str, wait_time_between_requests, proc_num, to_db, replace_existing))
            worker.daemon = True
            worker.start()
            workers.append(worker)

    for url in urls:
        time.sleep(wait_time_between_adding_urls)
        url_queue.put(url)
    # for i in range(num_threads):
    #     q.put('DONE')
    print('joining queue')
    url_queue.join()

    print('joining processes')
    [w.terminate() for w in workers]
