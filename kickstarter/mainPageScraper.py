import json
import logging
import pickle
import sqlite3
import time

import lxml
import requests
from fake_useragent import UserAgent
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_main_page_html(database_location):
    # import os

    # ff_profile = "/home/scrape/.mozilla/firefox/"
    # browser = splinter.Browser('firefox', profile=ff_profile)
    # if '/usr/local/bin' not in sys.path:
    #     sys.path.insert(0, '/usr/local/bin')
    # browser = splinter.Browser('phantomjs')
    driver = webdriver.Chrome()
    opts = Options()
    opts.add_argument(f"user-agent={UserAgent().chrome}")
    # opts.add_argument('headless')

    for i in range(5):
        try:
            driver.get('https://www.kickstarter.com/discover/advanced?sort=newest')
            break
        except:
            "Trying to load page again..."

    num_reloads = 0
    while "We're sorry, but something went wrong" in driver.title:
        time.sleep(5)
        driver.refresh()
        num_reloads += 1
        logging.debug("Oh no!")
        if num_reloads > 5:
            break

    # to make sure we don't load too many urls
    logging.debug("Getting projects in the big database")
    with sqlite3.connect(database_location) as db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute('select project.id as id from project order by launched_at desc limit 1000')
        all_project_ids = set([x['id'] for x in cur.fetchall()])
        all_project_ids = {62845062}
        logging.debug("Getting projects in the intermediate database")
        # urls_to_scrape = set([x['url'] for x in intermediate_db.query('select url from urls_to_scrape')])

        logging.debug("Starting download....")
        wait_time = 10
        wait_step = 3
        max_wait = 60
        max_tries = 3
        num_tries = 0
        num_projects = 0
        last_num_hrefs = 0
        extra_pages = None
        while True:
            # orig_html_length = len(browser.html)
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            button = driver.find_elements_by_xpath('//div[contains(@class, "load_more")]/a[@role="button"]')[-1]
            #        button = browser.find_by_xpath('//button[text()="Load more"]')
            #        browser.find_elements_by_link_text("Load more").click()
            button.click()
            time.sleep(wait_step)

            while True:
                project_data_elems = driver.find_elements_by_xpath('//div[@data-project]')
                num_projects = len(project_data_elems)
                if num_projects != last_num_hrefs:
                    break
                logging.debug("Waiting...")
                time.sleep(wait_step)
                wait_time += wait_step
                if wait_time >= max_wait: break
            if wait_time >= max_wait:
                num_tries += 1
                if num_tries >= max_tries:
                    break
            else:
                num_tries = 0
                last_num_hrefs = num_projects
            # Tony added next wo lines
            #       if num_hrefs > 5:
            #           print "{0} projects loaded".format(num_hrefs)
            #           break
            logging.debug("{0} projects loaded again".format(num_projects))
            last_project_data = json.loads(project_data_elems[-1].get_attribute('data-project'))
            last_project_id = last_project_data['id']
            logging.debug("last project id = {}".format(last_project_id))
            # last_project_id = last_project_id.replace("?ref=newest", "")
            # logging.debug("2 last_href = {}".format(last_project_id))
            if last_project_id in all_project_ids:  # or last_href in urls_to_scrape:
                if extra_pages is None:
                    extra_pages = 3
                else:
                    extra_pages += -1
                if extra_pages == 0:
                    break

        page_source = driver.page_source
        driver.quit()
        return page_source


def get_project_urls_from_main_page_html(page_source):
    tree = lxml.html.fromstring(page_source)
    urls = []
    for x in tree.xpath('//h6[@class="project-title"]/a'):
        url = x.attrib['href']
        if url[:11] != 'https://www':
            url = "https://www.kickstarter.com" + url
        url = url.replace("?ref=newest", "")
        urls.append(url)
    return urls


if __name__ == "__main__":
    max_last_page = 2
    num_extra_pages = 3

    with sqlite3.connect('kickstarter.db') as db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute('select project.id as id from project order by launched_at desc limit 1000')
        all_project_ids = set([x['id'] for x in cur.fetchall()])
    project_data = []
    for page_num in range(1, max_last_page + 1):
        r = requests.get(f'https://www.kickstarter.com/discover/advanced?sort=newest&page={page_num}')
        tree = html.fromstring(r.content)
        project_data_elems = tree.xpath('//div[@data-project]')
        project_data += [json.loads(x.attrib['data-project']) for x in project_data_elems]
        last_project_id = project_data[-1]['id']
        if last_project_id in all_project_ids:
            num_extra_pages -= 1
        if num_extra_pages < 0:
            break

    with open('main_page_raw_project_data.pickle', 'wb') as f:
        pickle.dump(project_data, f)
    # add_main_page_projects_to_intermediate_database()
