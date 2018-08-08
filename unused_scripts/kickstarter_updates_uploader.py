import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import ciso8601
import os
from unused_scripts import db_connections
import subprocess
import lxml.html
import mysql.connector
import json
from kickstarter_updater_2 import fast_upsert_many


def is_copyright_project(tree):
    try:
        # if this isn't found then it will trigger an exception
        text = tree.xpath('//div[@id="hidden_project"]//strong')[0].text
        assert ' is the subject of an intellectual property dispute and is currently unavailable.' in text
        return True
    except:
        return False


def get_updates_data(project_id, update_tree):
    if is_copyright_project(update_tree):
        raise ValueError()

    update_number = 1
    for i, entry in enumerate(update_tree.xpath('//div[@class="timeline"]/div')):
        update = dict()
        entry_class = entry.attrib['class']
        split_entry_class = entry_class.split(" ")
        if "timeline__divider--month" in split_entry_class:
            pass
        elif entry_class == "timeline__divider":
            pass
        elif "timeline__divider--cancellation" in split_entry_class:
            update['project_id'] = project_id
            update['post_unix_timestamp'] = int(ciso8601.parse_datetime(entry.xpath('.//time')[0].attrib['datetime']).timestamp())
            update['title'] = entry.xpath('.//div[@class="mb2"]/b')[0].text.strip()
            update['update_number'] = update_number
        elif "timeline__divider--failure" in split_entry_class:
            update['project_id'] = project_id
            update['post_unix_timestamp'] = int(ciso8601.parse_datetime(entry.xpath('.//time')[0].attrib['datetime']).timestamp())
            update['title'] = entry.xpath('.//div[@class="mb2"]/b')[0].text.strip()
            update['update_number'] = update_number
        elif "timeline__divider--successful" in split_entry_class:
            update['project_id'] = project_id
            update['post_unix_timestamp'] = int(ciso8601.parse_datetime(entry.xpath('.//time')[0].attrib['datetime']).timestamp())
            try:
                update['title'] = entry.xpath('.//div[@class="h3"]')[0].text.strip()
            except IndexError:
                update['title'] = 'Success'
            update['update_number'] = update_number
        elif "timeline__divider--launched" in split_entry_class:
            update['project_id'] = project_id
            update['post_unix_timestamp'] = int(ciso8601.parse_datetime(entry.xpath('.//time')[0].attrib['datetime']).timestamp())
            update['title'] = entry.xpath('.//div[@class="f2"]')[0].text.strip()
            update['update_number'] = update_number
        elif "timeline__item--right" in split_entry_class or "timeline__item--left" in split_entry_class:
            update['project_id'] = project_id
            update['post_unix_timestamp'] = int(ciso8601.parse_datetime(entry.xpath('.//time')[0].attrib['datetime']).timestamp())
            update['title'] = entry.xpath('.//h2[@class="grid-post__title"]')[0].text.strip()
            update['update_number'] = update_number
        elif "timeline__divider--potd" in split_entry_class:
            update['project_id'] = project_id
            update['post_unix_timestamp'] = int(ciso8601.parse_datetime(entry.xpath('.//time')[0].attrib['datetime']).timestamp())
            update['title'] = entry.xpath('./div/b')[0].text.strip()
            update['update_number'] = update_number
        else:
            raise Exception(
                "Unknown reward entry: {0}\nID of last project done: {1}".format(entry_class, project_id))

        if update.get('title') == "":
            raise Exception("No title: {0}".format(entry_class))

        if update != dict():
            update_number += 1
            yield update


def download_update_pages(skip_existing=True, only_updated_projects=False):
    db = db_connections.get_fungrosencrantz_schema('kickstarter_new')  # TODO: change after move

    orig_dir = os.getcwd()
    os.chdir('/mnt/data/scrape/kickstarter_updates')

    with open('all_urls', 'w') as f:
        for row in db.query('select url from all_files'):
            url = row['url']
            if url.endswith('/'):
                f.write(url + 'updates' + '\n')
            else:
                f.write(url + '/updates' + '\n')

    if only_updated_projects:
        raise NotImplementedError()
    else:
        if skip_existing:
            subprocess.call('wget -i all_urls --no-clobber --force-directories --output-file=wget.log', shell=True)
        else:
            subprocess.call('wget -i all_urls --timestamping --force-directories --output-file=wget.log', shell=True)

    os.chdir(orig_dir)


def update_project_table(only_live_projects=True):
    with open('../lib/fungrosencrantz_login', 'r') as f:
        fung_login = json.load(f)
    ks_conn = mysql.connector.connect(user=fung_login['username'], database='kickstarter_new',
                                      password=fung_login['password'], host=fung_login['hostname'],
                                      port=fung_login['port'])
    ks_cur = ks_conn.cursor()
    ks_cur.execute("SET SESSION sql_mode = 'TRADITIONAL';")

    if only_live_projects:
        ks_cur.execute('''
            SELECT
              id, url_project
            FROM project
            WHERE `state` = 'live'
            #limit 1000''')
        project_ids_urls_to_update = ks_cur.fetchall()
    else:
        ks_cur.execute('''
            SELECT
              id, url_project
            FROM project
            left join `update` on project.id = `update`.project_id
            WHERE `update`.project_id is null and state = 'canceled'
            group by project.id limit 1000;''')
        project_ids_urls_to_update = ks_cur.fetchall()

    ks_cur.close()
    ks_conn.close()

    orig_dir = os.getcwd()
    os.chdir('/mnt/data/scrape/kickstarter_updates')

    updates = []
    for id, url in project_ids_urls_to_update:
        file_path = os.path.join(url.replace('https://', ''), 'updates')
        if not os.path.exists(file_path):
            continue
        else:
            pass
            # print(id, url)
        assert url.startswith('https://')
        with open(file_path) as f:
            root = lxml.html.fromstring(f.read())
        try:
            update_chunk = [x for x in get_updates_data(project_id=id, update_tree=root)]
        except ValueError:
            update_chunk = []

        if len(update_chunk) == 0:
            raise Exception(f'Project with empty updates page: {url}')
        updates += update_chunk

    os.chdir(orig_dir)

    with open('../lib/fungrosencrantz_login', 'r') as f:
        fung_login = json.load(f)
    ks_conn = mysql.connector.connect(user=fung_login['username'], database='kickstarter_new',
                                      password=fung_login['password'], host=fung_login['hostname'],
                                      port=fung_login['port'])
    ks_cur = ks_conn.cursor()
    ks_cur.execute("SET SESSION sql_mode = 'TRADITIONAL';")
    fast_upsert_many(ks_cur, 'update', data=updates)
    ks_conn.commit()
    ks_cur.close()
    ks_conn.close()


if __name__ == '__main__':
    # download_update_pages(skip_existing=True, only_updated_projects=False)
    update_project_table(only_live_projects=False)
