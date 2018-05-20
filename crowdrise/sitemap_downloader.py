import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import spur
import datetime
import requests
import xml.etree.cElementTree
import subprocess
import logging
import traceback
import time
import useful_functions
import os
import re
import db_connections
import dateutil.parser
import psycopg2
import csv
import json
import lxml.html
from crowdrise_scraper import CrowdriseScraper


def download_sitemap_urls(output_filename='sitemap_urls.txt', ):
    logging.info('Downloading sitemap urls')
    subprocess.call(['wget https://www.crowdrise.com/sitemap/xml-index'], shell=True)
    with open('xml-index', 'r') as f:
        root = xml.etree.cElementTree.fromstring(f.read())
    os.remove('xml-index')
    # r = requests.get("https://www.crowdrise.com/sitemap/xml-index")
    # root = xml.etree.cElementTree.fromstring(r.content)

    site_map_locations = [x.text for x in root.findall('.//') if "www.crowdrise.com/sitemap/xml" in x.text]
    with open(output_filename, 'w') as f:
        for line in site_map_locations:
            if line[-1] == '/':
                line = line[:-1]
            if line.startswith('http:'):
                line = line.replace('http:', 'https:')
            if line == 'https://www.crowdrise.com':
                continue
            f.write(str(line) + '\n')


def clean_sitemap_table(db=None):
    if db is None:
        db = db_connections.get_fungrosencrantz_schema('crowdrise')
    q = """UPDATE sitemap
SET loc=left(loc, char_length(loc)-1)
WHERE right(loc, 1) = '/'
LIMIT 100000000000;"""
    db.query(q)

    q = """UPDATE sitemap
SET loc=REPLACE(loc, 'http:', 'https:')
WHERE loc LIKE 'http:%'
LIMIT 100000000000;"""
    db.query(q)

    q = """DELETE FROM sitemap
WHERE loc='https://www.crowdrise.com';"""
    db.query(q)


def get_all_pages_to_download(db=None):
    if db is None:
        db = db_connections.get_fungrosencrantz_schema('crowdrise')
    q = """SELECT
    CONCAT(t2.loc,
            '/fundraiser/',
            REPLACE(t2.loc,
                'https://www.crowdrise.com/',
                '')) AS loc
FROM
    sitemap AS t1
        JOIN
    sitemap AS t2 ON t1.loc = t2.loc
        AND t1.category = 'users'
        AND t2.category = 'fundraisers'
UNION
SELECT loc FROM sitemap;
    """
    logging.info('Querying database')
    r = db.query(q)
    logging.info('Writing URLs to file')
    with open('pages_to_download.txt', 'w') as f:
        for row in r:
            f.write('{}\n'.format(row['loc']))


CROWDRISE_URL_RE = re.compile('^(https://www.crowdrise.com([a-zA-Z0-9]|-|/|_|\.(?!\.))*)$', flags=re.IGNORECASE)


def upload_sitemap_data():
    logging.info('Uploading sitemap data to database')
    conn = db_connections.get_theta_postgres_db()
    cur = conn.cursor()
    cur.execute('set search_path = "backend"')
    cur.execute('''
        DROP TABLE IF EXISTS csv_raw_sitemap;
        CREATE TABLE csv_raw_sitemap
        (
            loc TEXT NOT NULL,
            category TEXT NOT NULL,
            lastmod TIMESTAMP WITH TIME ZONE,
            changefreq TEXT,
            priority NUMERIC,
            origin_file TEXT
        );''')
    logging.info('\traw data...')
    for category in os.listdir('www.crowdrise.com/sitemap/xml'):
        url_data = []
        for filename in os.listdir('www.crowdrise.com/sitemap/xml/{}'.format(category)):
            if filename.endswith('.swp'):  # nano file
                continue
            full_filename = 'www.crowdrise.com/sitemap/xml/{}/{}'.format(category, filename)
            logging.debug('current file = {}'.format(full_filename))
            with open(full_filename, 'rb') as f:
                print(full_filename)
                headers = []
                while True:
                    line = f.readline()
                    if line.strip() == b'':  # done with headers
                        break
                    headers.append(line)
                root = xml.etree.cElementTree.fromstring(f.read())

            root_tag = root.tag
            namespace = root_tag[root_tag.index('{'):root_tag.index('}') + 1]

            for url in root.findall('.//{}url'.format(namespace)):
                loc = url.find('./{}loc'.format(namespace))
                if loc is not None:
                    if loc.text is None:
                        loc = ''
                    else:
                        loc = loc.text.strip()
                lastmod = url.find('./{}lastmod'.format(namespace))
                if lastmod is not None:
                    lastmod = lastmod.text.strip()
                    if lastmod[0] not in ('1', '2'):  # if it's not from 1xxx or 2xxx, it's invalid date
                        lastmod = None
                changefreq = url.find('./{}changefreq'.format(namespace))
                if changefreq is not None:
                    changefreq = changefreq.text.strip()
                priority = url.find('./{}priority'.format(namespace))
                if priority is not None:
                    priority = priority.text.strip()
                if re.match(CROWDRISE_URL_RE, loc):
                    url_data.append((loc, category, lastmod, changefreq, priority, full_filename))
                else:
                    cur.executemany(
                        """insert into sitemap_bad_url values (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING""",
                        [(loc, category, changefreq, priority, full_filename, lastmod)])
        useful_functions.fast_insert_many(data=url_data, table='backend.csv_raw_sitemap', cur=cur)

    logging.info('\tgrouping by loc, category...')
    cur.execute('CREATE INDEX csv_raw_sitemap_loc_category_idx ON csv_raw_sitemap (loc, category);')
    cur.execute('''
        TRUNCATE table csv_sitemap;
        INSERT INTO csv_sitemap
          SELECT
            loc
            , category
            , max(lastmod)
            , max(changefreq)
            , max(priority)
            , max(origin_file)
          FROM csv_raw_sitemap AS t1
          GROUP BY loc, category; -- 36.7 seconds
        ''')
    logging.info('\tgrouping by loc...')
    cur.execute('''
        UPDATE csv_sitemap AS t2
        SET loc = CONCAT(t2.loc, '/fundraiser/', REPLACE(t2.loc, 'https://www.crowdrise.com/', ''))
        FROM csv_sitemap AS t1
            WHERE t1.loc = t2.loc
               AND t1.category = 'users'
               AND t2.category = 'fundraisers'
        ;
        UPDATE csv_sitemap
          SET loc = left(loc, char_length(loc)-1)
        WHERE loc LIKE '%/';
        INSERT INTO sitemap
          SELECT
            loc
            , array_agg(category) AS categories
            , max(lastmod) AS lastmod
            , max(changefreq) AS changefreq
            , max(priority) AS priority
            , array_agg(origin_file) AS origin_files
          FROM csv_sitemap
          GROUP BY loc
        ON CONFLICT (loc)
          DO UPDATE
            SET
              categories = ARRAY(SELECT DISTINCT unnest(array_cat(sitemap.categories, EXCLUDED.categories))),
              lastmod   = EXCLUDED.lastmod,
              changefreq  = EXCLUDED.changefreq,
              priority    = EXCLUDED.priority,
              origin_files = ARRAY(SELECT DISTINCT unnest(array_cat(sitemap.origin_files, EXCLUDED.origin_files)))
            WHERE sitemap.lastmod < EXCLUDED.lastmod;''')
    # cur.executemany("""
    # insert into backend.sitemap (loc, lastmod, changefreq, priority, category, origin_file) VALUES (%s, %s, %s, %s, %s, %s)
    # on conflict (loc, category) DO UPDATE
    #  set lastmod = EXCLUDED.lastmod,
    #      changefreq = EXCLUDED.changefreq,
    #      priority = EXCLUDED.priority,
    #      origin_file = EXCLUDED.origin_file
    #      where sitemap.lastmod != EXCLUDED.lastmod;""", url_data)
    '''
    cur.execute("""
        INSERT INTO backend.sitemap
          SELECT
            *
          FROM backend.csv_sitemap
        ON CONFLICT (loc, category)
          DO UPDATE
            SET lastmod   = EXCLUDED.lastmod,
              changefreq  = EXCLUDED.changefreq,
              priority    = EXCLUDED.priority,
              origin_file = EXCLUDED.origin_file
            WHERE sitemap.lastmod < EXCLUDED.lastmod;
    """)
    '''
    conn.commit()
    cur.close()
    conn.close()


def wget_urls(filename='sitemap_urls.txt', num_processes=1):
    wget_args = [
        # '--output-file=wget.log',
        # '--input-file={}'.format(filename),
        # '--limit-rate=2000k',
        # '--wait=.5',
        # '--random-wait=on',
        '--force-directories',
        '--no-clobber',
    ]
    processes = []
    if num_processes < 2:  # no longer supporting singe thread
        raise Exception('Number of processes has to be at least 2')
    elif num_processes == 1:
        subprocess.call(['wget', '--output-file=wget.log', '--input-file={}'.format(filename)] + wget_args)
    elif num_processes > 1:
        out_file_names = [filename + str(x) for x in range(num_processes)]
        split_text_file(filename, out_file_names)
        try:
            for p_id in range(num_processes):
                additional_args = [
                    '--output-file=wget{}.log'.format(p_id),
                    '--input-file={}'.format(filename + str(p_id)),
                ]
                if os.name == 'nt':
                    command = 'C:\\cygwin64\\bin\\run.exe wget {}'.format(" ".join(wget_args + additional_args))
                else:
                    command = 'wget {}'.format(" ".join(wget_args + additional_args))
                logging.info('Executing: {}'.format(command))
                processes.append(subprocess.Popen(command, shell=True))
            for p_id, process in enumerate(processes):
                logging.info('Waiting for {} (process {}) to complete'.format(process, p_id))
                process.wait()
        except:
            for process in processes:
                try:
                    logging.log('killing process {}'.format(process.pid))
                    process.kill()
                except:
                    pass
            raise
    else:
        raise Exception('Unknown value for processes: '.format(num_processes))


def split_text_file(in_file_name, out_file_names):
    with open(in_file_name, 'rb') as f_in:
        files_out = [open(x, 'wb') for x in out_file_names]
        for i, line in enumerate(f_in.readlines()):
            files_out[i % len(out_file_names)].write(line)
        [x.close() for x in files_out]


def create_directory_structure_for_urls(filename):
    logging.debug("reading file")
    with open(filename) as f:
        urls = f.readlines()
    logging.debug("getting corresponding folders")
    folders = [url[:url.rindex('/')].replace('https://', '') for url in urls]
    logging.debug("removing duplicates")
    folders = set(folders)
    logging.debug("creating directories")
    for folder in folders:
        useful_functions.ensure_directory('./' + folder)


def create_directory_structure_for_url_list(urls):
    logging.debug("getting corresponding folders")
    folders = [url[:url.rindex('/')].replace('https://', '') for url in urls]
    logging.debug("removing duplicates")
    folders = set(folders)
    logging.debug("creating directories")
    for folder in folders:
        useful_functions.ensure_directory('./' + folder)


def get_wget_log_data(log_text, num_urls_downloaded):
    # parses wget log data assuming they all are sent to a known file
    if type(log_text) == bytes:
        log_text = log_text.decode('utf8')
    # tz = ' +' + str(time.timezone // 3600)
    urls = []
    server_codes = []
    datetime_downloaded = []
    following_url_line = False
    following_response_line = False
    for i, line in enumerate(log_text.split('\n')):
        response_loc = line.find('response... ')
        if 'redirections exceeded' in line:
            following_response_line = False
            following_url_line = False
            server_codes[-1] += ' > Redirections Exceeded'
        elif line.endswith('[following]'):
            following_url_line = True
            following_response_line = True
            server_codes[-1] += ' > {}'.format(line.split()[1])
        elif line.startswith('--'):
            if following_url_line:
                following_url_line = False
            else:
                datetime_downloaded.append((' '.join(line.split()[:2])).replace('--', ''))
                urls.append(line.split()[-1])
                assert urls[-1].startswith('http')
        elif response_loc != -1:
            if following_response_line:
                following_response_line = False
                server_codes[-1] += ' > {}'.format(line[response_loc + len('response... '):])
            else:
                server_codes.append(line[response_loc + len('response... '):])

    assert len(urls) == num_urls_downloaded
    assert len(server_codes) == num_urls_downloaded
    assert len(datetime_downloaded) == num_urls_downloaded
    print(urls)
    print(server_codes)
    return urls, server_codes, datetime_downloaded


def get_wget_nv_log_data(log_text, num_urls_downloaded):
    # parses -nv log data
    # fails when there's authentication error because the url isn't printed in -nv logs
    log_text = log_text.decode('utf8')
    tz = ' +' + str(time.timezone // 3600)
    urls = []
    output_files = []
    error_codes = []
    datetime_downloaded = []
    for i, line in enumerate(log_text.split('\n')):
        if line.startswith('http') and line.endswith(':'):
            assert len(line.split()) == 1
            urls.append(line)
            output_files.append(None)
        if " ERROR " in line:
            datetime_downloaded.append(' '.join(line.split()[:2]) + tz)
            error_codes.append(int(line.split()[3][:-1]))
        if "URL:" in line and '->' in line:
            assert len(line.split()) == 7
            datetime_downloaded.append(' '.join(line.split()[:2]) + tz)
            url = line.split()[2][len('URL:'):]
            assert url.startswith('http')
            urls.append(url)
            error_codes.append(None)
            outfile = line.split()[5]
            assert outfile.startswith('"') and outfile.endswith('"')
            output_files.append(outfile[1:-1])
    assert len(urls) == num_urls_downloaded
    assert len(output_files) == num_urls_downloaded
    assert len(error_codes) == num_urls_downloaded
    assert len(datetime_downloaded) == num_urls_downloaded
    print(urls)
    print(error_codes)
    return urls, output_files, error_codes, datetime_downloaded


def update_html_table(limit=50):
    # DO NOT SET LIMIT ABOVE ~1000! It will hang indefinitely while transferring parsed wget log data due to size of str
    logging.info('updating html table...')
    conn = db_connections.get_theta_postgres_db()
    cur = conn.cursor()
    cur.execute('set search_path = "backend"')
    cur.execute('''
        SELECT sitemap.loc
        INTO TEMP urls_to_download
        FROM sitemap
          LEFT JOIN html
            ON sitemap.loc = html.loc
        WHERE (split_part(sitemap.loc, '/', 4) = split_part(sitemap.loc, '/', 6) OR split_part(sitemap.loc, '/', 6) = '')
              AND (html.loc IS NULL OR sitemap.lastmod > html.last_modified
                   OR html.last_modified IS NULL AND sitemap.lastmod IS NOT NULL)
        LIMIT {};
    '''.format(limit))
    cur.execute('SELECT count(*) FROM urls_to_download;')
    num_urls = cur.fetchall()[0][0]
    if num_urls == 0:  # all done
        cur.close()
        conn.close()
        return True
    cur.execute('''
        COPY urls_to_download TO '/tmp/crowdrise_urls_to_wget';''')
    with open('../lib/theta_login') as f:
        login = json.load(f)
    shell = spur.SshShell(missing_host_key=spur.ssh.MissingHostKey.accept, **login)
    logging.debug('done connecting ssh')
    with shell:  # \0x03 represents a newline in the data, | separates the url and html, \0x04 is |
        print('starting url download')
        time_before_download = time.time()
        try:
            shell.run(['wget',
                       '-O', '/hdd/crowdfunding/crowdrise/crowdrise_html_orig',
                       '-i', '/tmp/crowdrise_urls_to_wget',
                       '--output-file=/hdd/crowdfunding/crowdrise/wget_log',
                       # '-nv',
                       ], cwd='/hdd/crowdfunding/crowdrise/')
        except spur.results.RunProcessError as e:
            if e.return_code in (6, 8):  # 6 = Authentication error, 8 = server issued error response:
                pass
            else:
                raise
        print('download took {} seconds'.format(time.time() - time_before_download))
        logging.debug('done with wget')
        # interpret log
        lines = int(shell.run(['wc', '-l', '/tmp/crowdrise_urls_to_wget']).output.split()[
                        0])  # find how many urls were downloaded
        urls, server_codes, datetime_downloaded = get_wget_log_data(
            shell.run(['cat', '/hdd/crowdfunding/crowdrise/wget_log']).output,
            num_urls_downloaded=lines)
        shell.run(['sh', '-c',
                   r"tr '\n' '\03' < /hdd/crowdfunding/crowdrise/crowdrise_html_orig > /hdd/crowdfunding/crowdrise/crowdrise_html_2"])  # replace \n with \03
        shell.run(['sh', '-c',
                   r"tr '|' '\04' < /hdd/crowdfunding/crowdrise/crowdrise_html_2 > /hdd/crowdfunding/crowdrise/crowdrise_html_3"])  # replace | with \04
        shell.run(['sh', '-c',
                   r"tr '\r' '\05' < /hdd/crowdfunding/crowdrise/crowdrise_html_3 > /hdd/crowdfunding/crowdrise/crowdrise_html"])  # replace \r with \05 (to be removed)
        shell.run(['sh', '-c',
                   r"sed -i 's/<!doctype html/\n<!doctype html/I2g' /hdd/crowdfunding/crowdrise/crowdrise_html"])  # add before each document header, except first
        shell.run(['sh', '-c',
                   r"sed -i -e '$a\' /hdd/crowdfunding/crowdrise/crowdrise_html"])  # add a newline at the end of the file
        num_good_urls = len([x for x in server_codes if x[-2:] == 'OK'])
        assert num_good_urls == int(
            shell.run(['wc', '-l', '/hdd/crowdfunding/crowdrise/crowdrise_html']).output.split()[0])
        wget_data_file_text = ''
        for url, server_code, time_downloaded in zip(urls, server_codes, datetime_downloaded):
            if server_code[-2:] == 'OK':
                wget_data_file_text += r'{}|{}|{}\n'.format(url, server_code, time_downloaded)
            else:
                cur.execute(
                    """insert into html (loc, last_modified, server_code, html) VALUES ('{}', '{}', '{}', NULL)
                        ON CONFLICT (loc)
                          DO UPDATE
                            SET last_modified = EXCLUDED.last_modified,
                              server_code     = EXCLUDED.server_code;""".format(
                        url, time_downloaded, server_code
                    ))
        shell.run(['sh', '-c', 'printf "{}" > /hdd/crowdfunding/crowdrise/wget_data_file'.format(wget_data_file_text)])
        assert num_good_urls == int(
            shell.run(['wc', '-l', '/hdd/crowdfunding/crowdrise/wget_data_file']).output.split()[
                0])  # if this fails, make sure num_good_urls condition and the servercode condition are the same
        shell.run(['sh', '-c', r"rm /hdd/crowdfunding/crowdrise/crowdrise_html_orig"])  # remove temporary file
        shell.run(['sh', '-c',
                   r"paste -d '|' /hdd/crowdfunding/crowdrise/wget_data_file /hdd/crowdfunding/crowdrise/crowdrise_html > /hdd/crowdfunding/crowdrise/crowdrise_url_html"])  # combine url, timestamp, html
    logging.debug('done with shell')
    cur.execute("""
        DROP TABLE IF EXISTS html_staging;
        CREATE UNLOGGED TABLE html_staging
        (
          loc      TEXT PRIMARY KEY NOT NULL,
          server_code TEXT NOT NULL,
          last_modified TIMESTAMPTZ  DEFAULT NOW(),
          html     TEXT             NOT NULL
        );
        COPY html_staging FROM '/hdd/crowdfunding/crowdrise/crowdrise_url_html' WITH (
        DELIMITER E'|', ENCODING 'latin1', NULL '', ESCAPE E'\02', FORMAT CSV, QUOTE E'\01'  );
        INSERT INTO html (loc, last_modified, server_code, html)
          SELECT
              CASE WHEN right(loc, 1) = '/'
                THEN left(loc, -1)
              ELSE loc END
            , last_modified
            , server_code
            , REPLACE(REPLACE(REPLACE(html, E'\04', '|'), E'\03', E'\n'), E'\05', '')
          FROM html_staging
        ON CONFLICT (loc)
          DO UPDATE
            SET last_modified = EXCLUDED.last_modified,
              server_code     = EXCLUDED.server_code,
              html           = EXCLUDED.html;""")
    logging.debug('done with table copying')
    conn.commit()
    cur.close()
    conn.close()
    return False


def keep_theta_conn_alive(conn, cur):
    try:
        cur.execute('select 1;')
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        conn = db_connections.get_theta_postgres_db()
        cur = conn.cursor()
        cur.execute('set search_path = "backend"')

    return conn, cur


def scrape_new_html(limit=20, url_comment_id=dict(), test_url=None):
    theta_conn = db_connections.get_theta_postgres_db()
    theta_cur = theta_conn.cursor()
    theta_cur.execute('set search_path = "backend"')
    if test_url is not None:
        theta_cur.execute("select loc, html from html where loc = '{}';".format(test_url))
    else:
        theta_cur.execute(
            """
            SELECT
              html.loc
            , html
            FROM html
              JOIN sitemap
                ON html.loc = sitemap.loc
            WHERE (last_scrape IS NULL OR lastmod > last_scrape)
                  AND html IS NOT NULL
                  --AND NOT ('fundraisers' = ANY (categories))
                  AND NOT ('static' = ALL (categories) OR html.loc = 'https://www.crowdrise.com')
            limit {};""".format(
                limit))
    html_data = theta_cur.fetchall()
    if len(html_data) == 0:
        theta_cur.close()
        theta_conn.close()
        return True
    all_data = dict(
        fundraiser=[],
        user=[],
        charity=[],
        event=[],
        special_user=[],
        front_page_redirect=[],
        user_project=[],
        charity_event=[],
        team=[],
        donation=[]
    )
    scraped_urls = []
    for url, html in html_data:
        scraped_urls.append(url)
        try:
            # root = lxml.html.fromstring(lxml.html.tostring(lxml.html.fromstring(html.encode('latin1'))).decode('utf8'))
            try:
                root = lxml.html.fromstring(html.encode('latin1').decode('utf8'))
            except UnicodeDecodeError:
                logging.warning('unicode decode error for url "{}"'.format(url))
                theta_conn, theta_cur = keep_theta_conn_alive(theta_conn, theta_cur)
                theta_cur.execute('insert into html_bad_encoding values (%s) on CONFLICT DO NOTHING ;', [(url,)])
                theta_conn.commit()
                root = lxml.html.fromstring(html.encode('latin1').decode('utf8', errors='ignore'))
            try:
                page_type = CrowdriseScraper.get_page_type(root)
            except NotImplementedError:
                theta_conn, theta_cur = keep_theta_conn_alive(theta_conn, theta_cur)
                theta_cur.executemany("insert into unknown_page_type values (%s) on CONFLICT DO NOTHING;", [(url,)])
                theta_conn.commit()
                continue
            page_data = CrowdriseScraper.get_crowdrise_data(page_type, root, url, latest_comment_id=url_comment_id.get(url))
            if page_data is not None:
                # file_data['file_path'] = cur_file_name
                page_data['url'] = url
                page_data['true_url'] = root.xpath(
                    '//meta[@property="og:url"]')[0].attrib['content'].replace('https://', '').replace('http://', '')
                page_data['base_true_url'] = None

                # file_data['last_scrape'] = time.gmtime(os.path.getmtime(cur_file_name))

                # handle data that requires its own table - eg the fundraisers each user has
                if 'projects' in page_data.keys():
                    projects = page_data.pop('projects')
                    all_data['user_project'] += [{'username': page_data['username'],
                                                  'project': 'www.crowdrise.com' + x} for x in projects]
                if 'events' in page_data.keys():
                    events = page_data.pop('events')
                    all_data['charity_event'] += [{'charity': page_data['url'],
                                                   'event': 'www.crowdrise.com' + x} for x in events]
                if 'team_members' in page_data.keys():
                    team_members = page_data.pop('team_members')
                    all_data['team'] += team_members

                if 'donations' in page_data.keys():
                    donations = page_data.pop('donations')
                    all_data['donation'] += donations

                all_data[page_type].append(page_data)
        except:
            print('failed on url "{}"'.format(url))
            logging.error('failed on url "{}"'.format(url))
            raise
    all_data['user_project'] = [x for x in all_data['user_project'] if
                                re.match(CROWDRISE_URL_RE, 'https://' + x['project'])]
    db = db_connections.get_fungrosencrantz_schema('crowdrise')
    db_connections.multi_table_upload(data=all_data,
                                      db=db,
                                      ensure=True,
                                      process_num=None,
                                      chunk_size=3000)
    scrape_time = time.time()

    # update table with new entries
    db.query('truncate table _recently_updated')
    db.executable.execute('insert ignore into _recently_updated values (%s, %s)', [(x, scrape_time) for x in scraped_urls])
    db.executable.execute("""
        replace into crowdrise.funding_trend (url, username, amount_raised, goal, scrape_time_unix, type)
        SELECT
          fundraiser.url,
          CASE WHEN fundraiser_url IS NULL # individual fundraiser
            THEN fundraiser.username
          ELSE # team fundraiser
            '' # give team total raised for fundraiser, then use `team` to give individual contributions
          END,
          coalesce(team_total_raised, total_raised),
          NULL,
          _recently_updated.last_scrape_unix,
          'fundraiser'
        FROM fundraiser
          join _recently_updated on _recently_updated.url = fundraiser.url
          LEFT JOIN team ON fundraiser.url = team.fundraiser_url
        GROUP BY fundraiser.url;
        
        replace into crowdrise.funding_trend (url, username, amount_raised, goal, scrape_time_unix, type)
        select fundraiser_url, username, amount_raised, goal, _recently_updated.last_scrape_unix, 'team' from team
        join _recently_updated on _recently_updated.url = team.fundraiser_url;
        
        replace into crowdrise.funding_trend (url, username, amount_raised, goal, scrape_time_unix, type)
        select charity.url, '', money_raised, null, _recently_updated.last_scrape_unix, 'charity' from charity
        join _recently_updated on _recently_updated.url = charity.url;
        
        replace into crowdrise.funding_trend (url, username, amount_raised, goal, scrape_time_unix, type)
        select event.url, '', amount_raised, goal, _recently_updated.last_scrape_unix, 'event' from event
        join _recently_updated on _recently_updated.url = event.url;
        
        replace into crowdrise.funding_trend (url, username, amount_raised, goal, scrape_time_unix, type)
        select user.url, username, money_raised, null, _recently_updated.last_scrape_unix, 'user' from user
        join _recently_updated on _recently_updated.url = user.url;
        """)

    q = """
    update html
    set last_scrape = to_timestamp({})
    where loc in ({});""".format(scrape_time, ", ".join(["'" + x + "'" for x in scraped_urls]))
    theta_conn, theta_cur = keep_theta_conn_alive(theta_conn, theta_cur)
    theta_cur.execute(q)

    if test_url is None and limit != 0:
        theta_conn.commit()

    theta_cur.close()
    theta_conn.close()
    if len(html_data) < limit or test_url is not None:
        return False
    else:
        return True


if __name__ == '__main__':
    t0 = time.time()
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG, filename=log_file)  #
    logging.debug("Starting {}.".format(sys.argv[0]))
    try:
        import os

        # os.chdir('e:/kickstarter')
        # wget_urls('ks_urls.txt', num_processes=4)

        download_sitemap_urls('sitemap_urls.txt')
        subprocess.call(['wget',
                         '-i', 'sitemap_urls.txt',
                         '--force-directories',
                         '--save-headers',
                         '--timestamping',
                         '--output-file=wget_log',
                         '--limit-rate=10M'])
        upload_sitemap_data()
        while not update_html_table(700):
            pass
        # update_html_table(1000)
        '''
        with open('../lib/theta_login') as f:
            login = json.load(f)
        shell = spur.SshShell(missing_host_key=spur.ssh.MissingHostKey.accept, **login)
        lines = int(shell.run(['wc', '-l', '/tmp/crowdrise_urls_to_wget']).output.split()[
                        0])  # find how many urls were downloaded
        urls, error_codes, datetime_downloaded = get_wget_log_data(
            shell.run(['cat', '/hdd/crowdfunding/crowdrise/wget_log']).output,
            num_urls_downloaded=lines)
        '''

        # '''
        # db = db_connections.get_fungrosencrantz_schema('crowdrise_new')
        # r = db.executable.execute('select url, max(id) from crowdrise_new.donation group by url')
        url_donation_id = dict()
        # for row in r:
        #    url_donation_id[row[0]] = row[1]
        while scrape_new_html(limit=1000, url_comment_id=url_donation_id,
                              # test_url='https://www.crowdrise.com/thephilomenaproject'
                              ):  # be sure download comments later...
            pass
        # '''


    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        useful_functions.send_scott_a_text('Crowdrise download error', 'crowdrise')
        raise
    logging.info('{} has completed in {} seconds.'.format(sys.argv[0], time.time() - t0))
