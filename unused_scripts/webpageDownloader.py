import tablib
from requests_futures.sessions import FuturesSession
from lxml import html
import time
import concurrent.futures
import requests
import dataset
from unused_scripts import db_connections
import logging


def get_html_from_urls(urls, workers=4, initial_xpath=None, verbose_level=3, append=""):
    """
    :param urls: urls to be downloaded (list)
    :param workers: how many connections to make at once (int)
    :param initial_xpath: reduce the html to a subset based on this xpath string
    :param verbose_level: how often the function will print what it's doing. 3 is the highest. (int)
    :param append: appended to the end of each url (string)
    :return: html_responses, num_passses
            html_responses is a dictionary where the keys are urls and the values are their htmls
                html is either html of the page or the error code as a string
            num_passes is how many passes it took to download the html from the urls
    """
    logging.debug("\tStarting download.")
    html_responses = dict()
    for url in urls:
        html_responses[url] = None
    num_passes = 0
    while None in html_responses.values():
        sleep_time = 1

        logging.debug("\tCreating session")
        session = FuturesSession(max_workers=workers)
        pages = dict()
        for url in urls:
            if html_responses[url] is None:
                pages[url] = session.get(url + append)
        logging.debug("\tGetting responses.")
        for key in pages:
            p = pages[key]
            pass
            try:
                logging.debug("\t\tGetting response")
                response = p.result(timeout=10)
                logging.debug("\t\tDone getting response")
                if response.status_code != 200:
                    if response.status_code == 404:  # page not found
                        result = "404"
                    elif response.status_code == 429:  # 429 is overwhelmed page
                        result = None
                    else:
                        logging.debug("Error code {0}".format(response.status_code))
                        result = str(response.status_code)
                else:
                    result = response.text
                    if initial_xpath is not None:
                        tree = html.fromstring(result).xpath(initial_xpath)[0]
                        result = html.tostring(tree)
            except concurrent.futures.TimeoutError:
                logging.debug("\tTimeout")
                result = None
            except requests.exceptions.ConnectionError:
                logging.debug("\tRequests Timeout")
                sleep_time += 2
                result = None
            html_responses[key] = result
        none_count = 0
        for key in html_responses:
            if html_responses[key] is None:
                none_count += 1
        logging.debug("\tThere were {0} blank pages this time. Sleep time = {1}".format(none_count, sleep_time))
        del session
        time.sleep(min(sleep_time, 6))
        num_passes += 1
    logging.debug("\tDownload took {0} passes.".format(num_passes))
    return html_responses, num_passes


# overwrites if check_if_already_downloaded is True, ignores if it's False
def urls_to_database(base_urls=set(), url_append="/reward", db_connector=dataset.connect, db_args=dict(),
                     html_table_name='reward_html', chunk_size=104, max_workers=8, verbose_level=0,
                     auto_adjust_workers=False, check_if_already_downloaded=True, b_insert=True):
    """
    :param base_urls: base urls (list)
    :param url_append: appended to the end of each url (string)
    :param db_connector: function that connects to the output db
    :param db_args: passed to db_connector as **db_args (dict)
    :param html_table_name: output table name (string)
    :param chunk_size: how many urls to download at once (int)
    :param max_workers: how many connections to make at once (int)
    :param verbose_level: how often the function will print what it's doing. 3 is the highest. (int)
    :param auto_adjust_workers: automatically adjust number of workers based on number of passes (bool)
                                tries to keep the number of passes between 3 and 10
    :param check_if_already_downloaded: remove urls that are already in the output table (bool)
    :param b_insert: if true, assume urls can be inserted as opposed to updating (bool)
    :return: None
    """
    logging.debug("Getting urls already in output table")

    # remove urls that are already in the output table, if desired
    if check_if_already_downloaded:
        html_db = db_connector(**db_args)
        logging.debug("html_db = {}".format(html_db))
        if html_table_name in html_db.tables:
            urls_in_output_table = set([url['url'] for url in html_db[html_table_name]['url']])
            logging.debug("Removing these urls from input")
            # delete all urls already in the output table
            if type(base_urls) != set:
                base_urls = set(base_urls)
            base_urls = base_urls - urls_in_output_table

    if type(base_urls) != list:
        base_urls = list(base_urls)
    current_offset = 0
    while current_offset < len(base_urls):
        logging.debug("Generating urls to download.")
        cur_urls = [x + url_append for x in base_urls[current_offset:current_offset + chunk_size]]
        logging.debug("Position {0} out of {1}".format(current_offset, len(base_urls)))
        html_db = db_connector(**db_args)
        #        html_table = html_db.get_table(html_table_name, primary_id='url', primary_type='VARCHAR(190)')
        html_table = html_db.get_table(html_table_name, primary_id='url', primary_type='String')
        html_responses, num_passes = get_html_from_urls(cur_urls, workers=max_workers, verbose_level=verbose_level)
        if auto_adjust_workers:
            # decrease max_workers if it takes more than 10 passes, to a min of 3 workers
            if num_passes > 10: max_workers = max(max_workers - 1, 2)
            # increase max_workers if it takes fewer than 3 passes, to a max of 40 workers
            if num_passes < 3: max_workers = min(max_workers + 1, 40)
            logging.debug("max_workers = {}".format(max_workers))
        data = tablib.Dataset(headers=['url', 'html'])
        for url in html_responses:
            # get rid of what was appended to the url. Note html_responses still expects the appended url.
            if url_append is not None and len(url_append) > 0:
                url = url[:len(url) - len(url_append)]
            # each row is of the form projectid, base_url, html
            row = [url, html_responses[url + url_append]]
            data.append(row)
        logging.debug("table name={}".format(html_table_name))
        if b_insert:  # don't have to worry about repeats
            html_table.insert_many(data.dict)
        else:
            db_connections.uploadOutputFile(data=data.dict, db=html_db, table=html_table_name, strict=False)

        del data
        current_offset += chunk_size


if __name__ == "__main__":
    pass
