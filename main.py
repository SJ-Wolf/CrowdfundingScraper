import time
from kiva import newest_scraper
import sys
import logging
import useful_functions
import traceback
from utils.download_utils import get_url, get_latest_free_proxy_list, download_urls_through_proxies, get_proxy_server_session
import sqlite3
from kickstarter import kickstarter_updater
import lxml.html


def try_run_function(f, failed_text):
    try:
        f()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        try:
            useful_functions.send_scott_a_text(message=failed_text)
        except Exception:
            logging.error('Message sending failed too!')
            logging.error(traceback.format_exc())
        raise


if __name__ == '__main__':
    t0 = time.time()
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG, filename=log_file)
    logging.debug("Starting {}.".format(sys.argv[0]))

    # try_run_function(newest_scraper.update, 'Kiva update failed!')
    try_run_function(kickstarter_updater.update, 'Kickstarter update failed!')

    # with sqlite3.connect('kickstarter.db') as db:
    #     cur = db.cursor()
    #     cur.execute('select id from category where parent_id is null')
    #     categories = [x[0] for x in cur.fetchall()]
    #     for category in categories:
    #         for goal_num in range(5):
    #             for raised_num in range(3):
    #                 projects = kickstarter_updater.get_short_project_data_from_main_page(
    #                     0, goal_num=goal_num, raised_num=raised_num,
    #                     category_id=category, sort_by='end_date',
    #                     max_last_page=50, start_page=1)
    #                 print(f'{len(projects)} projects')
    #                 cur.executemany('insert or ignore into all_files values (?, ?)', ((x['id'], x['urls']['web']['project']) for x in projects))
    #                 db.commit()
    #                 print(category, goal_num, raised_num)

    # requests_session = get_proxy_server_session(1)
    # project_ids, project_urls = kickstarter_updater.get_live_projects()
    # print(len(project_ids))
    # project_html_iterator = (
    #     kickstarter_updater.get_raw_project_data_from_tree(lxml.html.fromstring(get_url(url, requests_session=requests_session, verbose=True, overwrite=True)))
    #     for url in project_urls)
    # # wget_urls(project_urls, overwrite=True, folder='html')
    # kickstarter_updater.parse_kickstarter_files(chunksize=1000, limit=None,
    #                                             raw_project_data_iterator=project_html_iterator)

    # with sqlite3.connect(DATABASE_LOCATION) as db:
    #     cur = db.cursor()
    #     utils.sqlite_utils.delete_temporary_tables(cur)
    # get_comments()
    # get_short_creator_bios

    # urls = get_url('http://169.229.7.239/list.txt', overwrite=False).split('\n')
    # proxies = get_latest_free_proxy_list(refresh=True)
    # download_urls_through_proxies(proxy_strings=proxies, urls=urls,
    #                               wait_time_between_requests=1,
    #                               wait_time_between_adding_urls=0.1, to_db=False, replace_existing=False, connections_per_server=5)

    # import random
    # urls = get_url('http://169.229.7.239/list.txt', overwrite=False).split('\n')
    # print(len(urls))
    # random.shuffle(urls)
    # urls = urls[:1]
    # total_content_length = 0
    # for url in urls:
    #     r = requests.head(url)
    #     total_content_length += int(r.headers['Content-Length'])
    # print(total_content_length / len(urls))

    # with sqlite3.connect(DATABASE_LOCATION) as db:
    #     cur = db.cursor()
    #     cur.execute('select url_project from project order by id limit 3000, 1000')
    #     urls = [x[0] for x in cur.fetchall()]
    # print(urls)
    # project_html_iterator = (get_raw_project_data_from_tree(lxml.html.fromstring(page_source))
    #                          for page_source in get_urls(urls, per_second=10.0, overwrite=True, max_num_proxies=-1))
    # print('parsing')
    # parse_kickstarter_files(chunksize=1000, limit=None,
    #                         raw_project_data_iterator=project_html_iterator)

    # for url, file_name in ((get_raw_project_data_from_file(f)['urls']['web']['project'], f) for f in get_files_in_directory(r'C:\Users\kyle1\html_old\www.kickstarter.com')):
    #     with open(file_name, 'r', encoding='utf8') as f:
    #         file_contents = f.read()
    #         with sqlite3.connect('html.db') as db:
    #             cur = db.cursor()
    #             cur.execute('insert or ignore into html values (?, ?)', (url, file_contents))

    # with sqlite3.connect('html_old.db') as db:
    #     cur = db.cursor()
    #     cur.execute('select page_source from html')
    #     page_sources = [x[0] for x in cur.fetchall()]
    #     comments_to_delete = []
    #     for p in page_sources:
    #         tree = lxml.html.fromstring(p)
    #         new_comments = parse_comment_tree(tree, 0)
    #         for comment in new_comments:
    #             comments_to_delete.append(comment['id'])
    #     with sqlite3.connect(DATABASE_LOCATION) as ks_db:
    #         ks_cur = ks_db.cursor()
    #         with utils.sqlite_utils.tmp_table(pd.DataFrame(comments_to_delete, columns=['comment_id']), ks_db) as tmp_table_name:
    #             ks_cur.execute(f'CREATE UNIQUE INDEX "{tmp_table_name}_comment_id_uindex" ON "{tmp_table_name}" (comment_id)')
    #             ks_cur.execute(f'delete from comments where exists(select 1 from "{tmp_table_name}" as t where t.comment_id = comments.id)')

    # with sqlite3.connect('R:/kickstarter.db') as db:
    #     pd.read_sql("""
    #         select
    #           coalesce(c2.parent_id, c1.parent_id, c1.id) as parent_category_id,
    #           coalesce(c3.name, c2.name, c1.name)         as parent_category_name,
    #           project.*
    #         from project
    #           join category as c1 on project.category_id = c1.id
    #           left join category as c2 on c1.parent_id = c2.id
    #           left join category as c3 on c2.parent_id = c3.id;""", db).to_csv('project.tsv', sep='\t', index=False)
    # with sqlite3.connect(DATABASE_LOCATION) as db:
    #     pd.read_sql("""select * from category;""", db).to_csv('category.tsv', sep='\t', index=False)
    # with sqlite3.connect(DATABASE_LOCATION) as db:
    #     pd.read_sql("""select * from location;""", db).to_csv('location.tsv', sep='\t', index=False)
    # with sqlite3.connect(DATABASE_LOCATION) as db:
    #     pd.read_sql("""
    #         select
    #           id,
    #           projectid,
    #           length(body) as num_chars,
    #           CASE WHEN length(body) >= 1
    #             THEN
    #               (length(body) - length(replace(body, ' ', ''))) + 1
    #           ELSE
    #             0
    #           END          as num_words
    #         from comments;""", db).to_csv('comments.tsv', sep='\t', index=False)

    logging.info('{} has completed.'.format(sys.argv[0]))
    print(time.time() - t0)
