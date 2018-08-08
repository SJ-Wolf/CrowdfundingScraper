import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import os
from unused_scripts import db_connections
import subprocess
from utils.useful_functions import split_array_into_chunks
import shutil


def delete_redundant_directories():
    db = db_connections.get_fungrosencrantz_schema('crowdrise')

    urls = [x['url'] + '/*' for x in db.query('select url from fundraiser where url not like "%/fundraiser/%";')]

    urls_split = split_array_into_chunks(urls, 10000)

    for chunk in urls_split:
        with open('directories_to_remove', 'w') as f:
            f.write(" ".join(chunk))
        subprocess.call('rm -Rf `cat directories_to_remove`', shell=True)


def delete_all_directories():
    dir_path = 'www.crowdrise.com'
    for i, directory in enumerate(os.walk(dir_path).next()[1]):
        shutil.rmtree(os.path.join('www.crowdrise.com', directory))


def rename_files():
    file_index = 0
    for root, directories, filenames in os.walk('www.crowdrise.com'):
        for filename in filenames:
            file_index += 1
            cur_file_name = os.path.join(root, filename)
            if file_index % 100 == 0:
                print
                file_index
            if filename[-2] == '.':
                shutil.move(src=cur_file_name, dst=cur_file_name[:-2])


def clean_url_list():
    with open('urls_to_scrape') as f:
        urls = [x[:-1] for x in f.readlines()]

    good_urls = []
    for i, u in enumerate(urls):
        file_path = u.replace('https://', '')

        # check if the path has a folder in it
        # if it does, ignore since these are only special pages
        # also ignore any files that already exist
        if os.path.split(file_path)[0] == 'www.crowdrise.com':
            if not os.path.exists(file_path):
                good_urls.append(urls[i])

    good_urls = set(good_urls)
    with open('urls_to_scrape2', 'w') as f:
        for u in good_urls:
            f.write(u + '\n')


clean_url_list()
