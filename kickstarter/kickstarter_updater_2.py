# from kickstarter_updater import *
import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import webrobots_download
import lxml.etree
import csv
import time
from datetime import datetime
import sqlite3
import requests
import mysql.connector
import lxml.html
import regex as re
import json
import codecs
import logging
import os
import subprocess
import db_connections
import pickle
import mainPageScraper

url = 'https://www.kickstarter.com/projects/jpeteranetz/an-american-apocalypse-the-reckoning'


# print(lxml.html.tostring(root.xpath('//div[contains(@class,"js-full-description")]')[0], encoding='unicode', method='text'))
# items? livestreams? profile : feature_image_attributes?

def from_req():
    r = requests.get(url)
    main_tree = lxml.html.fromstring(r.text)
    r = requests.get(url + '/updates')
    update_tree = lxml.html.fromstring(r.content)
    r = requests.get(url + '/description')
    desc_tree = lxml.html.fromstring(r.content)
    r = requests.get(url + '/rewards')
    reward_tree = lxml.html.fromstring(r.content)

    p = ProjectScraper(main_tree, desc_tree)
    r = RewardScraper(reward_tree, p.dict['id'])
    u = UpdateScraper(update_tree, p)
    p.dict['url'] = url
    return p, r, u


def from_int():
    tables = ('description_html', 'main_html', 'reward_html', 'update_html')
    intermediate_db = db_connections.get_intermediate_db()
    page_sources = dict()
    for table in tables:
        page_sources[table.replace("_html", '')] = intermediate_db[table].find_one(url=url)['html']

    main_tree = lxml.html.fromstring(page_sources['main'])
    desc_tree = lxml.html.document_fromstring(page_sources['description'])
    reward_tree = lxml.html.fromstring(page_sources['reward'])
    update_tree = lxml.html.fromstring(page_sources['update'])

    p = ProjectScraper(main_tree, desc_tree)
    r = RewardScraper(reward_tree, p.dict['id'])
    u = UpdateScraper(update_tree, p)
    p.dict['url'] = url
    return p, r, u


# http://stackoverflow.com/a/24519338
ESCAPE_SEQUENCE_RE = re.compile(r'''
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )''', re.UNICODE | re.VERBOSE)


def decode_escapes(s):
    def decode_match(match):
        return codecs.decode(match.group(0), 'unicode-escape')

    return ESCAPE_SEQUENCE_RE.sub(decode_match, s)


CURRENT_PROJECT_PATTERN = re.compile(r'current_project = ".*";')


def get_raw_project_data_from_tree(tree):
    current_project_str = None
    for script in tree.xpath('//script'):
        current_project_search = CURRENT_PROJECT_PATTERN.search(lxml.html.tostring(script, encoding='unicode'))
        if current_project_search is not None:
            assert current_project_str is None
            current_project_str = current_project_search.group(0)[len('current_project = "'):-2]
    assert current_project_str is not None
    current_project_data = json.loads(
        lxml.html.tostring(lxml.html.fromstring(decode_escapes(current_project_str)), method='text',
                           encoding='unicode'))
    return current_project_data


def get_raw_project_data_from_file(file_name):
    with open(file_name, 'rb') as f:
        current_project_str = None
        for event, script in lxml.etree.iterparse(f, tag='script', encoding='utf8', html=True, ):
            current_project_search = CURRENT_PROJECT_PATTERN.search(lxml.html.tostring(script, encoding='unicode'))
            if current_project_search is not None:
                assert current_project_str is None
                current_project_str = current_project_search.group(0)[len('current_project = "'):-2]
                current_project_data = json.loads(
                    lxml.html.tostring(lxml.html.fromstring(decode_escapes(current_project_str)), method='text',
                                       encoding='unicode'))
                break
        assert current_project_str is not None
    return current_project_data


def get_project_data_from_file(file_name):
    return get_project_data(get_raw_project_data_from_file(file_name))


def get_project_data(raw_project_data):
    project_data = dict()

    for key in ('blurb', 'comments_count', 'country', 'created_at', 'currency', 'currency_symbol',
                'currency_trailing_code', 'deadline', 'disable_communication', 'goal', 'id'
                , 'launched_at', 'name', 'pledged', 'slug', 'spotlight', 'staff_pick', 'state'
                , 'state_changed_at', 'static_usd_rate', 'successful_at', 'updated_at', 'updates_count'
                , 'usd_pledged', 'backers_count'):
        if key in raw_project_data.keys():
            project_data[key] = raw_project_data.pop(key)
        else:
            project_data[key] = None

    category_data = raw_project_data.pop('category')
    creator_data = raw_project_data.pop('creator')
    items = raw_project_data.pop('items')
    for i in range(len(items)):
        items[i]['project_id'] = project_data['id']
    livestreams = raw_project_data.pop('livestreams')
    for i in range(len(livestreams)):
        livestreams[i]['project_id'] = project_data['id']
    location_data = raw_project_data.get('location')

    reward_data_list = raw_project_data.pop('rewards')
    for i in range(len(reward_data_list)):
        reward_data_list[i]['project_id'] = project_data['id']

    project_data['category_id'] = category_data['id']
    project_data['creator_id'] = creator_data['id']
    project_data['location_id'] = location_data['id'] if location_data is not None else None
    project_data['photo_url'] = raw_project_data['photo'].get('1536x864')
    project_data['profile_background_color'] = raw_project_data['profile'].get('background_color')
    project_data['profile_background_image_opacity'] = raw_project_data['profile'].get('background_image_opacity')
    project_data['profile_blurb'] = raw_project_data['profile'].get('blurb')
    project_data['profile_id'] = raw_project_data['profile'].get('id')
    project_data['profile_link_background_color'] = raw_project_data['profile'].get('link_background_color')
    project_data['profile_link_text'] = raw_project_data['profile'].get('link_text')
    project_data['profile_link_text_color'] = raw_project_data['profile'].get('link_text_color')
    project_data['profile_link_url'] = raw_project_data['profile'].get('link_url')
    project_data['profile_name'] = raw_project_data['profile'].get('name')
    project_data['profile_project_id'] = raw_project_data['profile'].get('project_id')
    project_data['profile_should_show_feature_image_section'] = raw_project_data['profile'].get(
        'should_show_feature_image_section')
    project_data['profile_show_feature_image'] = raw_project_data['profile'].get('show_feature_image')
    project_data['profile_state'] = raw_project_data['profile'].get('state')
    project_data['profile_state_changed_at'] = raw_project_data['profile'].get('state_changed_at')
    project_data['profile_text_color'] = raw_project_data['profile'].get('text_color')
    project_data['url_project'] = raw_project_data['urls']['web']['project']
    project_data['url_project_short'] = raw_project_data['urls']['web']['project_short']
    project_data['url_rewards'] = raw_project_data['urls']['web']['rewards']
    project_data['url_updates'] = raw_project_data['urls']['web']['updates']
    project_data['video_id'] = raw_project_data['video']['id'] if raw_project_data.get('video') is not None else None
    project_data['video_url_high'] = raw_project_data['video']['high'] if raw_project_data.get(
        'video') is not None else None
    project_data['video_url_webm'] = raw_project_data['video']['webm'] if raw_project_data.get(
        'video') is not None else None
    project_data['video_height'] = raw_project_data['video']['height'] if raw_project_data.get(
        'video') is not None else None
    project_data['video_width'] = raw_project_data['video']['width'] if raw_project_data.get(
        'video') is not None else None
    project_data['video_status'] = raw_project_data['video']['status'] if raw_project_data.get(
        'video') is not None else None
    # for key in project_data:
    #    if key.endswith('_at') and project_data[key] is not None:
    #        project_data[key] = datetime.fromtimestamp(project_data[key])
    # project_data['deadline'] = datetime.fromtimestamp(project_data['deadline'])
    project_data['file_name'] = project_data['url_project'].replace('https://', '').replace('http://', '')
    return project_data, category_data, creator_data, items, livestreams, location_data, reward_data_list


def insert_list(cur, table, data, insert_mode='insert'):
    column_str = None
    data_to_insert = []
    data_keys = list(data[0].keys())  # order doesn't matter
    for row in data:
        tmp_column_str = ''
        value_list = []
        for i, key in enumerate(data_keys):
            if i == 0:
                tmp_column_str += '{}'.format(key)
            else:
                tmp_column_str += ', {}'.format(key)
            value_list.append(row[key])
        if column_str is None:
            column_str = tmp_column_str
        else:
            assert column_str == tmp_column_str
        data_to_insert.append(value_list)
    cur.executemany('{} into {} ({}) values ({})'.format(
        insert_mode,
        table,
        column_str,
        '%s, ' * (len(data_to_insert[0]) - 1) + ' %s'
    ), data_to_insert)


def test_url():
    p, r, u = from_req()

    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    kickstarter_db.query("SET SESSION sql_mode = 'TRADITIONAL'")

    db_connections.multi_table_upload(data=dict(project=[p.dict],
                                                update=u.updates,
                                                reward=r.rewards),
                                      db=kickstarter_db
                                      )


def download_all_urls():
    with open('../lib/fungrosencrantz_login', 'r') as f:
        fung_login = json.load(f)

    ks_conn = mysql.connector.connect(user=fung_login['username'], database='kickstarter_new',
                                      password=fung_login['password'], host=fung_login['hostname'],
                                      port=fung_login['port'],
                                      use_unicode=True, charset='utf8mb4')
    ks_cur = ks_conn.cursor()
    ks_cur.execute("SET SESSION sql_mode = 'TRADITIONAL';")
    int_conn = sqlite3.connect('html_database.db')
    int_cur = int_conn.cursor()
    int_cur.execute('SELECT url FROM urls_to_scrape')
    all_urls = [x[0] for x in int_cur.fetchall()]
    print(all_urls[:10])

    all_project_data = []
    t0 = time.time()
    for url in all_urls[:20]:
        r = requests.get(url)
        main_tree = lxml.html.fromstring(r.text)
        all_project_data.append(get_project_data(main_tree))
    insert_list(cur=ks_cur, table='project', data=all_project_data, insert_mode='replace')
    print('Time to download and insert: {}'.format(time.time() - t0))
    ks_conn.commit()
    ks_cur.close()
    ks_conn.close()
    int_cur.close()
    int_conn.close()


def parse_kickstarter_file(url):
    url = url.replace('https://', '').replace('http://', '')
    with open(url, 'r', encoding='utf8') as f:
        f_text = f.read()
        return get_project_data(lxml.html.fromstring(f_text))


def fast_upsert_many(cur, table, data=[]):
    if len(data) == 0:
        return
    cur.execute("""
        SELECT `COLUMN_NAME`
        FROM `INFORMATION_SCHEMA`.`COLUMNS`
        WHERE `TABLE_SCHEMA`=schema()
            AND `TABLE_NAME`='{}';""".format(table))
    columns = [x[0] for x in cur.fetchall()]
    with open('/tmp/mysql_insert.csv', 'w', newline='') as f:
        raw_files_writer = csv.DictWriter(f, fieldnames=columns, restval='', delimiter='\t', escapechar='',
                                          lineterminator='\n', extrasaction='raise')
        try:
            raw_files_writer.writerows(data)
        except ValueError:
            print(table)
            raise
    set_vars_str = ''
    for i, col in enumerate(columns):
        if i == 0:
            set_vars_str += '@v' + col
        else:
            set_vars_str += ', @v' + col
    set_columns_str = ''
    for i, col in enumerate(columns):
        if i != 0:
            set_columns_str += ',\n'
        set_columns_str += "`{0}` = nullif(@v{0}, '')".format(col)
    q = r'''
        load data local infile '/tmp/mysql_insert.csv' replace into table `{}`
        character set 'utf8mb4'
        fields TERMINATED BY '\t' escaped by '' optionally enclosed by '"'
        lines TERMINATED BY '\n'
        ({})
        SET
        {}
        ;'''.format(table, set_vars_str, set_columns_str)
    # print(q)
    cur.execute(q)
    cur.execute('show warnings;')
    warnings = [x for x in cur.fetchall() if not x[2].startswith('Duplicate entry')]
    if len(warnings) > 0:
        print(data)
        print(table)
        print(warnings)
        raise Exception('Warnings raised during upload.')
    return


def flatten_data(data):
    flattened_data = []
    for row in data:
        if row is None:
            continue
        flattened_data.append(flatten_dict(row))
    return flattened_data


def flatten_reward_data(data):
    flattened_reward_data = flatten_data(data)
    flattened_reward_data[:] = (x for x in flattened_reward_data if x['id'] != 0)
    flattened_rewards_items_data = []
    for i, row in enumerate(flattened_reward_data):
        if 'rewards_items' in row.keys():
            flattened_rewards_items_data += flatten_data(row.pop('rewards_items'))
    return flattened_reward_data, flattened_rewards_items_data


def create_text_table_for_data(data, cur, table):
    all_keys = set()
    for row in data:
        all_keys = all_keys.union(set(row.keys()))
    cur.execute('drop table if exists {};'.format(table))
    if len(all_keys) == 0:
        raise Exception('No keys to create table with.')
    q = 'create table {} (\n'.format(table)
    for i, key in enumerate(list(all_keys)):
        if i == 0:
            q += '\t`{}` TEXT\n'.format(key)
        else:
            q += '\t, `{}` TEXT\n'.format(key)
    q += ');'
    cur.execute(q)


def get_files_in_directory(directory='.'):
    for dirpath, dirnames, filenames in os.walk(directory):
        for fname in filenames:
            full_filename = os.path.join(dirpath, fname)
            yield full_filename


def print_warnings(cur):
    cur.execute('show warnings;')
    print(cur.fetchall())


def parse_kickstarter_files(files=None, chunksize=100, limit=None, directory=None):
    def upload_all():
        # flattened_all_reward_data = flatten_data(all_reward_data)
        flattened_all_reward_data, flattened_all_rewards_items_dat = flatten_reward_data(all_reward_data)
        flattened_all_category_data = flatten_data(all_category_data)
        flattened_all_creator_data = flatten_data(all_creator_data)
        flattened_all_item_data = flatten_data(all_item_data)
        flattened_all_livestream_data = flatten_data(all_livestream_data)
        flattened_all_location_data = flatten_data(all_location_data)
        # create_text_table_for_data(flattened_all_reward_data, ks_cur, 'reward')
        # create_text_table_for_data(flattened_all_rewards_items_dat, ks_cur, 'reward_item')
        # create_text_table_for_data(flattened_all_category_data, ks_cur, 'category')
        # create_text_table_for_data(flattened_all_creator_data, ks_cur, 'creator')
        # create_text_table_for_data(flattened_all_item_data, ks_cur, 'item')
        # create_text_table_for_data(flattened_all_location_data, ks_cur, 'location')
        # create_text_table_for_data(flattened_all_livestream_data, ks_cur, 'livestream')
        fast_upsert_many(cur=ks_cur, table='reward', data=flattened_all_reward_data)
        fast_upsert_many(cur=ks_cur, table='reward_item', data=flattened_all_rewards_items_dat)
        fast_upsert_many(cur=ks_cur, table='category', data=flattened_all_category_data)
        fast_upsert_many(cur=ks_cur, table='creator', data=flattened_all_creator_data)
        fast_upsert_many(cur=ks_cur, table='item', data=flattened_all_item_data)
        fast_upsert_many(cur=ks_cur, table='livestream', data=flattened_all_livestream_data)
        fast_upsert_many(cur=ks_cur, table='location', data=flattened_all_location_data)
        fast_upsert_many(cur=ks_cur, table='project', data=all_project_data)

    with open('../lib/fungrosencrantz_login', 'r') as f:
        fung_login = json.load(f)
    ks_conn = mysql.connector.connect(user=fung_login['username'], database='kickstarter_new',
                                      password=fung_login['password'], host=fung_login['hostname'],
                                      port=fung_login['port'])
    ks_cur = ks_conn.cursor()
    ks_cur.execute("SET SESSION sql_mode = 'TRADITIONAL';")

    if files is None:  # use all_files table
        q = """
            SELECT
              trim(replace(replace(all_files.url, 'https://', ''), '\r', ''))
            FROM all_files
              LEFT JOIN project
                ON all_files.project_id = project.id
            WHERE project.id IS NULL
            order by project_id"""
        if limit:
            q += ' LIMIT {}'.format(limit)
        q += ';'
        ks_cur.execute(q)
        files = [x[0] for x in ks_cur.fetchall()]

    file_num = 0
    all_project_data = []
    all_category_data = []
    all_creator_data = []
    all_item_data = []
    all_livestream_data = []
    all_location_data = []
    all_reward_data = []
    for full_filename in files:
        if directory:
            full_filename = os.path.join(directory, full_filename)
        if not os.path.exists(full_filename):
            logging.warning(full_filename + ' not found; skipping')
            continue
        file_num += 1
        project_data, category_data, creator_data, items, livestreams, location_data, reward_data_list = get_project_data_from_file(
            full_filename)
        project_data['file_name'] = full_filename
        all_project_data.append(project_data)
        all_category_data.append(category_data)
        all_creator_data.append(creator_data)
        all_item_data += items
        all_livestream_data += livestreams
        all_location_data.append(location_data)
        all_reward_data += reward_data_list

        if len(all_project_data) >= chunksize:
            try:
                upload_all()
                ks_conn.commit()
            except:
                raise
            all_project_data = []
        if limit is not None and file_num == limit:
            break

    upload_all()
    ks_cur.execute("""
        INSERT INTO funding_trend
          SELECT
            id,
            last_modification,
            usd_pledged,
            backers_count,
            updates_count,
            comments_count,
            state
          FROM kickstarter_new.project
            LEFT JOIN funding_trend
              ON project.id = funding_trend.projectid AND project.last_modification = funding_trend.project_last_modification
          WHERE funding_trend.projectid IS NULL;""")
    ks_conn.commit()
    ks_cur.close()
    ks_conn.close()


def flatten_dict(d, prepend=''):
    def get_prepend():
        if prepend == '':
            return prepend
        else:
            return prepend + '_'

    new_dict = dict()
    for key, item in d.items():
        if type(item) == dict:
            sub_dict = flatten_dict(item, prepend=get_prepend() + key)
            for sub_key in sub_dict:
                if sub_key in new_dict.keys():
                    assert new_dict[sub_key] == sub_dict[sub_key]
                else:
                    new_dict[sub_key] = sub_dict[sub_key]
        else:
            new_dict[get_prepend() + key] = item
    return new_dict


def get_sorted_project_paths():
    # all_projects_sorted.csv exists
    project_paths = []
    with open('all_projects_sorted.csv') as f:
        csv_reader = csv.reader(f)
        for project_id, url in csv_reader:
            project_paths.append(url.replace('https://', ''))
    return project_paths


def get_sorted_downloaded_project_paths(skip=None):
    project_paths = get_sorted_project_paths()
    if skip:
        project_paths = project_paths[skip:]
    for i, f_name in enumerate(project_paths):
        if os.path.exists(f_name):
            yield f_name


def upload_new_projects_list():
    page_source = mainPageScraper.get_main_page_html()
    with open('main_page_source.pickle', 'wb') as f:
        pickle.dump(page_source, f)
    # with open('main_page_source.pickle', 'rb') as f:
    #     page_source = pickle.load(f)

    tree = lxml.html.fromstring(page_source)
    projects = []
    project_card_elements = tree.xpath('//div[@class="row"]/li/div')
    for card_element in project_card_elements:
        project_id = int(card_element.attrib['data-project_pid'])
        project_url = card_element.xpath('.//h6[@class="project-title"]/a')[0].attrib['href']
        if project_url[:11] != 'https://www':
            project_url = "https://www.kickstarter.com" + project_url
        project_url = project_url.replace("?ref=newest", "")
        projects.append((project_id, project_url))
    theta_db = db_connections.get_fungrosencrantz_schema('kickstarter_new')  # TODO: change to kickstarter after move
    theta_db.executable.execute("insert ignore into all_files values (%s, %s)", projects)


def upload_webrobots_downloads(download_directory='/home/scrape/downloads'):
    projects = webrobots_download.projects_from_webrobots(download_directory)
    theta_db = db_connections.get_fungrosencrantz_schema('kickstarter_new')  # TODO: change to kickstarter after move
    theta_db.executable.execute("insert ignore into all_files values (%s, %s)", projects)


def wget_urls(urls, overwrite=True):
    if not overwrite:
        raise NotImplementedError('Not overwriting not implemented yet.')
    with open('urls_to_download', 'w') as f:
        for u in urls:
            f.write(u + '\n')
    subprocess.call(['wget -N --force-directories -i urls_to_download --output-file=wget_log'], shell=True)


def get_new_and_live_projects():
    q = '''
        SELECT id, min(url) as url
        FROM (
               SELECT
                 id,
                 url_project AS url
               FROM project
               WHERE state = 'live'
               UNION
               SELECT
                 all_files.project_id,
                 all_files.url
               FROM all_files
                 LEFT JOIN project
                   ON all_files.project_id = project.id
               WHERE project.id IS NULL
             ) AS t1
        GROUP BY id
        ORDER BY id;'''
    db = db_connections.get_fungrosencrantz_schema('kickstarter_new')  # TODO: change after move
    new_and_live_projects = [(x['id'], x['url']) for x in db.query(q)]
    return [x[0] for x in new_and_live_projects], [x[1] for x in new_and_live_projects], [x[1].replace('https://', '') for x in new_and_live_projects]


if __name__ == '__main__':
    t0 = time.time()
    # upload_webrobots_downloads()
    upload_new_projects_list()
    project_ids, project_urls, project_paths = get_new_and_live_projects()

    orig_dir = os.getcwd()
    os.chdir('/mnt/data/scrape')
    for path in project_paths:
        if os.path.exists(path):
            os.remove(path)
    wget_urls(project_urls)
    os.chdir(orig_dir)

    parse_kickstarter_files(files=project_paths, chunksize=1000, limit=None, directory='/mnt/data/scrape')
    print(time.time() - t0)
