import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By

import time
import json
import logging
import useful_functions
import traceback


def indiegogo_request(per_page=100, pg_num=1, project_status='all', percent_funded='all', location='everywhere',
                      quick_filter='trending'):
    arguments = locals()
    request = 'https://www.indiegogo.com/private_api/explore?'
    for arg in arguments.keys():
        request += '&{}={}'.format(arg, arguments[arg])
    return request


def run():
    browser = webdriver.Firefox()
    time_between_requests = 5
    json_storage_directory = 'front_page_json'
    useful_functions.ensure_directory(json_storage_directory)
    error_dump_file = 'newest_scraper_error_output.txt'

    cur_page = 1
    while True:
        initial_request_time = time.time()
        browser.get(indiegogo_request(pg_num=cur_page))
        timeout = 15  # seconds
        try:
            element_present = EC.presence_of_element_located((By.XPATH, '//pre'))
            WebDriverWait(browser, timeout).until(element_present)
        except TimeoutException:
            logging.error("Loading took too much time!")
            with open(error_dump_file, 'w') as f:
                f.write(browser.page_source)
            browser.close()
            raise

        projects = json.loads(browser.find_element_by_xpath('//pre').text)['campaigns']

        if len(projects) == 0:
            break

        with open('{}/{}.json'.format(json_storage_directory, cur_page), 'w') as f:
            json.dump(projects, f)
        cur_page += 1
        while initial_request_time + time_between_requests > time.time():
            time.sleep(.3)
    browser.close()


if __name__ == '__main__':
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG, filename=log_file)
    logging.debug("Starting {}.".format(sys.argv[0]))
    try:
        run()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        raise
    logging.info('{} has completed.'.format(sys.argv[0]))
