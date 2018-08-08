import csv
import json
import os

import lxml.html
import requests

from utils.download_utils import wget_urls


def projects_from_webrobots(csv_location='/home/scrape/downloads'):
    orig_dir = os.getcwd()
    os.chdir(csv_location)  # where the csvs are stored
    all_projects = []
    for dir_path, dir_names, file_names in os.walk('.'):
        for f_name in file_names:
            with open(os.path.join(dir_path, f_name), encoding='utf8') as f:
                if f_name.endswith('.csv'):
                    csv_reader = csv.DictReader(f, dialect='unix')
                    for i, line in enumerate(csv_reader):
                        # print(line[0])
                        all_projects.append(
                            (int(line['id']),
                             json.loads(line['urls'])['web']['project'].split('?')[0].replace('http://', 'https://'))
                        )
    os.chdir(orig_dir)
    return all_projects


def get_all_webrobots_csv_urls():
    if os.path.exists('all_webrobot_urls.txt'):
        with open('all_webrobot_urls.txt') as f:
            return [x.strip() for x in f.readlines()]
    else:
        url = 'https://webrobots.io/kickstarter-datasets/'
        r = requests.get(url)
        tree = lxml.html.fromstring(r.content)
        with open('all_webrobot_urls.txt', 'w') as f:
            urls = [x.attrib['href'] for x in tree.xpath('//a') if x.text == 'CSV']
            f.writelines('\n'.join(urls))
            return urls


def download_webrobots_csvs():
    folder = 'webrobots_downloads'
    urls = get_all_webrobots_csv_urls()
    wget_urls(urls, overwrite=False, folder=folder)
    print("Unzip them using 7zip now!")
