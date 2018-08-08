# from kickstarter_updater import *
# import sys
import codecs
import csv
import json
import logging
import os
import pickle
import sqlite3
import urllib.parse

import lxml.etree
import lxml.html
import pandas as pd
import regex as re
import dateparser
import requests

import utils.sqlite_utils
from kickstarter import webrobots_download
from utils.download_utils import get_url, get_urls

DATABASE_LOCATION = 'kickstarter.db'

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
    """
    Structures raw data from Kickstarter

    :param raw_project_data: project data straight from kickstarter
    :return: project_data, category_data, creator_data, items, livestream_list, location_data, reward_data_list
        \n\t_data means dictionary (ie one row of a table)
        \n\t_list mean list of dictionaries (i.e. multiple rows of a table)
    """
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
    item_list = raw_project_data.pop('items')
    for i in range(len(item_list)):
        item_list[i]['project_id'] = project_data['id']
    livestream_list = raw_project_data.pop('livestreams') if 'livestreams' in raw_project_data else []
    for i in range(len(livestream_list)):
        livestream_list[i]['project_id'] = project_data['id']
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
    project_data['video_url_webm'] = raw_project_data['video'].get('webm') if raw_project_data.get(
        'video') is not None else None
    project_data['video_height'] = raw_project_data['video']['height'] if raw_project_data.get(
        'video') is not None else None
    project_data['video_width'] = raw_project_data['video']['width'] if raw_project_data.get(
        'video') is not None else None
    project_data['video_status'] = raw_project_data['video']['status'] if raw_project_data.get(
        'video') is not None else None
    project_data['file_name'] = project_data['url_project'].replace('https://', '').replace('http://', '')
    return project_data, category_data, creator_data, item_list, livestream_list, location_data, reward_data_list


def fast_upsert_many(conn, table_name: str, data: list):
    """
    Upserts data into table
    :param conn: database connection to pass to Pandas
    :param table_name:
    :param data: list of dictionaries
    :return:
    """
    if len(data) == 0:
        return
    df = pd.DataFrame(data)
    utils.sqlite_utils.insert_into_table(df, table_name, conn, replace=True)


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


def print_warnings(cur):
    cur.execute('show warnings;')
    print(cur.fetchall())


def get_project_data_from_dir(directory):
    for dir_path, dir_names, file_names in os.walk(directory):
        for f_name in file_names:
            full_filename = os.path.join(dir_path, f_name)
            if not os.path.exists(full_filename):
                logging.warning(full_filename + ' not found; skipping')
                continue
            yield get_raw_project_data_from_file(full_filename)


def parse_kickstarter_files(raw_project_data_iterator, chunksize=100, limit=None):
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
        fast_upsert_many(conn=db, table_name='reward', data=flattened_all_reward_data)
        fast_upsert_many(conn=db, table_name='reward_item', data=flattened_all_rewards_items_dat)
        fast_upsert_many(conn=db, table_name='category', data=flattened_all_category_data)
        fast_upsert_many(conn=db, table_name='creator', data=flattened_all_creator_data)
        fast_upsert_many(conn=db, table_name='item', data=flattened_all_item_data)
        fast_upsert_many(conn=db, table_name='livestream', data=flattened_all_livestream_data)
        fast_upsert_many(conn=db, table_name='location', data=flattened_all_location_data)
        fast_upsert_many(conn=db, table_name='project', data=all_project_data)

    with sqlite3.connect(DATABASE_LOCATION) as db:
        cur = db.cursor()

        file_num = 0
        all_project_data = []
        all_category_data = []
        all_creator_data = []
        all_item_data = []
        all_livestream_data = []
        all_location_data = []
        all_reward_data = []
        for raw_project_data in raw_project_data_iterator:
            file_num += 1
            project_data, category_data, creator_data, items, livestreams, location_data, reward_data_list = get_project_data(
                raw_project_data)
            project_data['file_name'] = None
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
                    db.commit()
                except:
                    raise
                all_project_data = []
            if limit is not None and file_num == limit:
                break

        upload_all()
        cur.execute("""
            INSERT INTO funding_trend
              SELECT
                id,
                last_modification,
                usd_pledged,
                backers_count,
                updates_count,
                comments_count,
                state
              FROM project
                LEFT JOIN funding_trend
                  ON project.id = funding_trend.projectid AND project.last_modification = funding_trend.project_last_modification
              WHERE funding_trend.projectid IS NULL;""")


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


def get_live_projects(limit=None):
    q = '''
        SELECT id, min(url) as url
        FROM (
               SELECT
                 id,
                 url_project AS url
               FROM project
               WHERE state = 'live'
             ) AS t1
        GROUP BY id
        ORDER BY id;'''
    with sqlite3.connect(DATABASE_LOCATION) as db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute(q)
        new_and_live_projects = [(x['id'], x['url']) for x in cur.fetchall()]
    if limit is not None:
        new_and_live_projects = new_and_live_projects[:limit]
    return [x[0] for x in new_and_live_projects], [x[1] for x in new_and_live_projects]


def get_new_projects(limit=None):
    q = '''
        SELECT id, min(url) as url
        FROM (
               SELECT
                 all_files.project_id as id,
                 all_files.url
               FROM all_files
                 LEFT JOIN project
                   ON all_files.project_id = project.id
               WHERE project.id IS NULL
             ) AS t1
        GROUP BY id
        ORDER BY id;'''
    with sqlite3.connect(DATABASE_LOCATION) as db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute(q)
        new_and_live_projects = [(x['id'], x['url']) for x in cur.fetchall()]
    if limit is not None:
        new_and_live_projects = new_and_live_projects[:limit]
    return [x[0] for x in new_and_live_projects], [x[1] for x in new_and_live_projects]


def add_webrobots_csvs_to_all_files():
    with open('tmp.pickle', 'rb') as f:
        projects = webrobots_download.projects_from_webrobots('webrobots_downloads')
    with sqlite3.connect(DATABASE_LOCATION) as db:
        cur = db.cursor()
        cur.executemany('insert or ignore into all_files values (?, ?)', projects)


def build_front_page_request(page_num, sort_by='newest', category_id=None, goal_num=None, raised_num=None):
    params = dict()
    params['sort'] = sort_by
    params['page'] = page_num
    if category_id:
        params['category_id'] = category_id
    if goal_num:
        params['goal'] = goal_num
    if goal_num:
        params['raised'] = raised_num
    url = f'https://www.kickstarter.com/discover/advanced'
    return url + '?' + urllib.parse.urlencode(params)


def get_short_project_data_from_main_page(stop_at_unix_timestamp, start_page=1,
                                          max_last_page=200, sort_by='newest',
                                          category_id=None, goal_num=None, raised_num=None):
    raw_project_data = []
    for page_num in range(start_page, max_last_page + start_page):
        r = requests.get(build_front_page_request(page_num, sort_by=sort_by, category_id=category_id, goal_num=goal_num, raised_num=raised_num))
        tree = lxml.html.fromstring(r.content)
        project_data_elems = tree.xpath('//div[@data-project]')
        if len(project_data_elems) == 0 or tree.xpath('//b[contains(@class, "count")]/text()')[0].strip() == '0 projects':
            print('No projects!')
            break
        raw_project_data += [json.loads(x.attrib['data-project']) for x in project_data_elems]
        if sort_by == 'newest':
            last_project_launched_at = raw_project_data[-1]['launched_at']
        else:
            last_project_launched_at = raw_project_data[-1]['deadline']
        if last_project_launched_at < stop_at_unix_timestamp:
            break
    return raw_project_data


def onlyNumerics(seq):
    return int(''.join(filter(type(seq).isdigit, seq.split(".")[0])))


def parse_comment_tree(tree, projectid):
    comments = []
    comment_sections = tree.xpath('//li[@class="page"]/ol[@class="comments"]/li')

    for c_section in comment_sections:
        comment = dict()
        comment['projectid'] = projectid
        comment['id'] = onlyNumerics(c_section.attrib['id'])
        user_section = c_section.xpath('.//a[contains(@class, "author")]')
        if len(user_section) == 1:
            comment['user_id'] = user_section[0].attrib['href'].replace("/profile/", '')
            comment['user_name'] = user_section[0].text
        else:
            assert c_section.xpath('.//span[contains(@class, "author")]')[0].text == 'deleted'
            comment['user_id'] = None
            comment['user_name'] = None

        comment['body'] = "\n".join(c_section.xpath('.//p/text()'))
        try:
            comment['post_date'] = c_section.xpath('.//time')[0].attrib['datetime'].split('T')[0].replace('"', '')
        except IndexError:
            comment['post_date'] = dateparser.parse(c_section.xpath('.//span[contains(@class, "date")]/a/text()')[0]).strftime("%Y-%m-%d")
        badge_results = c_section.xpath('.//*[contains(@*, "-badge")]')
        if len(badge_results) > 0:
            badge_string = ' '.join([x[:-len('-badge')] for x in badge_results[0].attrib['class'].split(' ') if x.endswith('-badge')])
        else:
            badge_string = None
        comment['badge'] = badge_string

        if comment['user_id'] == "":
            raise Exception("Blank user_id")
        comments.append(comment)
    return comments


def get_comments_from_result(project_id, url, latest_comment_id):
    complete_url = url + f'/comments?cursor={latest_comment_id}&direction=asc'
    print(project_id, complete_url, latest_comment_id)
    tree = lxml.html.fromstring(get_url(complete_url))
    return parse_comment_tree(tree, project_id)


def get_short_creator_bios():
    def try_scrape_single_elem(tree, xpath):
        elems = tree.xpath(xpath)
        if len(elems) == 0:
            return None
        if len(elems) == 1:
            return elems[0]
        raise Exception('Too many found for xpath "{xpath}"')

    def upload_results(creator_external_url_list, creator_bio_list):
        with sqlite3.connect(DATABASE_LOCATION) as db:
            cur = db.cursor()
            if len(creator_external_url_list) > 0:
                cur.executemany('insert or ignore into creator_external_url values (?, ?, ?, ?)', creator_external_url_list)
            if len(creator_bio_list) > 0:
                cur.executemany('insert or ignore into creator_bio (creator_id, location_name) values (?, ?)', creator_bio_list)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS creator_external_url
        (
          creator_id int,
          href       text,
          target     text,
          link_text  text,
          PRIMARY KEY (creator_id, href)
        )
    """
    get_bios_sql = """
        select
          creator_id,
          min(project.url_project) || '/creator_bio'
        from project
        where not exists(select 1
                         from creator_bio
                         where project.creator_id = creator_bio.creator_id)
        group by creator_id
        order by creator_id
        ;"""

    with sqlite3.connect(DATABASE_LOCATION) as db:
        cur = db.cursor()
        cur.execute(create_table_sql)
        cur.execute(get_bios_sql)
        results = cur.fetchall()

    creator_external_url_list = []
    creator_bio_list = []

    urls = [x[1] for x in results]
    creator_ids = [x[0] for x in results]

    # with Parallel(n_jobs=2) as parallel:
    #     for id_url_pagesource_list in chunks(
    #             zip(creator_ids, urls, get_urls(urls, per_second=5.0, overwrite=False, max_num_proxies=10, refresh_proxy_list=False, skip_not_existing=True)),
    #             chunk_size=14):
    #         parallel(
    #             delayed(print)(creator_id, url) for creator_id, url, page_source in id_url_pagesource_list)
    # return

    for i, (creator_id, url, page_source) in enumerate(
            zip(urls, creator_ids,
                get_urls(urls, per_second=5.0, overwrite=False, max_num_proxies=10, refresh_proxy_list=False, skip_not_existing=False))):  # now parse..
        if page_source is None:
            continue
        tree = lxml.html.fromstring(page_source)
        location = try_scrape_single_elem(tree, '//p[@class="f5 bold mb0"]/text()')
        creator_bio_list.append((creator_id, location))
        links_list_elems = tree.xpath('//ul[contains(@class, "links")]//a')
        for a_elem in links_list_elems:
            creator_external_url_list.append((creator_id, a_elem.attrib['href'], a_elem.attrib.get('target'), a_elem.text))
        if i % 5000 == 4999:
            print(i)
            upload_results(creator_external_url_list, creator_bio_list)
            creator_external_url_list = []
            creator_bio_list = []
    upload_results(creator_external_url_list, creator_bio_list)


def get_comments():
    for i in range(100000):
        with sqlite3.connect(DATABASE_LOCATION) as db:
            completed_projects = set()
            cur = db.cursor()
            cur.execute("""
                select
                  id,
                  url_project,
                  IFNULL((select max(comments.id)
                          from comments
                          where comments.projectid = project.id), 0)
                from project
                where comments_count - IFNULL(deleted_comments, 0) > 0
                      and comments_count - IFNULL(deleted_comments, 0) != (select count(*)
                                                                           from comments
                                                                           where comments.projectid = project.id)
                limit 2000
                """)
            comments = []
            results = [list(x) for x in cur.fetchall()]
            if len(results) == 0:
                break
            while True:
                if len(completed_projects) == len(results):
                    break
                if len(comments) > 100000:
                    break
                for result_index, (project_id, url, latest_comment_id) in enumerate(results):
                    if project_id in completed_projects:
                        continue
                    complete_url = url + f'/comments?cursor={latest_comment_id}&direction=asc'
                    print(project_id, complete_url, latest_comment_id)
                    tree = lxml.html.fromstring(get_url(complete_url, wait_time=0))
                    new_comments = parse_comment_tree(tree, project_id)
                    if len(new_comments) == 0:
                        completed_projects.add(project_id)
                    else:
                        results[result_index][-1] = new_comments[0]['id']  # set latest_comment_id
                        comments += new_comments

            fast_upsert_many(db, 'comments', comments)

            df = pd.DataFrame(list(completed_projects), columns=['project_id'])
            with utils.sqlite_utils.tmp_table(df, db) as tmp_table_name:
                cur.execute(f"""
                    update project
                    set deleted_comments = comments_count - (select count(*)
                                                             from comments
                                                             where projectid = project.id)
                    where exists(select 1
                                 from "{tmp_table_name}" t
                                 where t.project_id = project.id);""")
    utils.sqlite_utils.delete_temporary_tables(DATABASE_LOCATION)


def update():
    with sqlite3.connect(DATABASE_LOCATION) as db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute('select * from project order by created_at desc limit 1')
        stop_at_end_date = int(cur.fetchall()[0]['launched_at']) - 1 * 24 * 60 * 60  # add a buffer of 1 day
        logging.debug(f'Stopping at timestamp {stop_at_end_date}')

    with sqlite3.connect('kickstarter.db') as db:
        cur = db.cursor()
        projects = get_short_project_data_from_main_page(
            stop_at_unix_timestamp=stop_at_end_date, max_last_page=200, start_page=1)
        print(f'{len(projects)} projects')
        cur.executemany('insert or ignore into all_files values (?, ?)', ((x['id'], x['urls']['web']['project']) for x in projects))
        db.commit()

    urls = get_new_projects()[1]
    urls += get_live_projects()[1]
    urls = list(set(urls))
    print(urls)
    logging.debug(f'downloading {len(urls)} urls...')
    if len(urls) > 1000:
        num_proxies = -1
        per_second = 10
    else:
        num_proxies = 50
        per_second = 5

    project_html_iterator = (get_raw_project_data_from_tree(lxml.html.fromstring(page_source))
                             for page_source in get_urls(urls, per_second=per_second, overwrite=True, max_num_proxies=num_proxies))
    logging.debug('parsing')
    parse_kickstarter_files(chunksize=1000, limit=None,
                            raw_project_data_iterator=project_html_iterator)
    logging.debug('downloading comments')
    get_comments()
    utils.sqlite_utils.delete_temporary_tables(DATABASE_LOCATION)


def add_old_projects_to_all_files():
    """
    Adds project ids/urls from long ago to all_files. Run this before update() to add the new projects to the database.
    :return:
    """
    with sqlite3.connect('kickstarter.db') as db:
        cur = db.cursor()
        cur.execute('select id from category where parent_id is null')
        categories = [x[0] for x in cur.fetchall()]
        for category in categories:
            for goal_num in range(5):
                for raised_num in range(3):
                    projects = get_short_project_data_from_main_page(
                        0, goal_num=goal_num, raised_num=raised_num,
                        category_id=category, sort_by='end_date',
                        max_last_page=200, start_page=1)
                    print(f'{len(projects)} projects')
                    cur.executemany('insert or ignore into all_files values (?, ?)', ((x['id'], x['urls']['web']['project']) for x in projects))
                    db.commit()
                    print(category, goal_num, raised_num)
