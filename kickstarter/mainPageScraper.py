import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')

import json
from lxml import html
import lxml
import db_connections
import pickle
import logging
import splinter
import time


def get_main_page_html():
    # import os

    # ff_profile = "/home/scrape/.mozilla/firefox/"
    # browser = splinter.Browser('firefox', profile=ff_profile)
    if '/usr/local/bin' not in sys.path:
        sys.path.insert(0, '/usr/local/bin')
    browser = splinter.Browser('phantomjs')

    for i in range(5):
        try:
            browser.visit('https://www.kickstarter.com/discover/advanced?sort=newest')
            break
        except:
            "Trying to load page again..."

    num_reloads = 0
    while browser.title.find("We're sorry, but something went wrong") != -1:
        time.sleep(5)
        browser.reload()
        num_reloads += 1
        logging.debug("Oh no!")
        if num_reloads > 5: break

    # to make sure we don't load too many urls
    logging.debug("Getting projects in the big database")
    project_db = db_connections.get_fungrosencrantz_schema('kickstarter_new')  # TODO: change back to kickstarter after move
    all_project_urls = set([x['url'] for x in project_db.query('select project.url_project as url from project order by launched_at desc limit 1000')])
    logging.debug("Getting projects in the intermediate database")
    intermediate_db = db_connections.get_intermediate_db()
    # urls_to_scrape = set([x['url'] for x in intermediate_db.query('select url from urls_to_scrape')])

    logging.debug("Starting download....")
    wait_time = 10
    wait_step = 3
    max_wait = 60
    max_tries = 3
    num_tries = 0
    num_hrefs = 0
    last_num_hrefs = 0
    extra_pages = None
    while True:
        orig_html_length = len(browser.html)
        button = browser.find_by_xpath('//div[@class="load_more"]/a[@role="button"]')[-1]
        #        button = browser.find_by_xpath('//button[text()="Load more"]')
        #        browser.find_elements_by_link_text("Load more").click()
        button.click()
        time.sleep(wait_step)

        while True:
            project_hrefs = browser.find_by_xpath('//h6[@class="project-title"]/a')
            num_hrefs = len(project_hrefs)
            if num_hrefs != last_num_hrefs:
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
            last_num_hrefs = num_hrefs
        # Tony added next wo lines
        #       if num_hrefs > 5:
        #           print "{0} projects loaded".format(num_hrefs)
        #           break
        logging.debug("{0} projects loaded again".format(num_hrefs))
        last_href = project_hrefs[-1]['href']
        logging.debug("last_href = {}".format(last_href))
        last_href = last_href.replace("?ref=newest", "")
        logging.debug("2 last_href = {}".format(last_href))
        if last_href in all_project_urls:  # or last_href in urls_to_scrape:
            if extra_pages is None:
                extra_pages = 3
            else:
                extra_pages += -1
            if extra_pages == 0:
                break

    page_source = browser.html
    browser.quit()
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


def upload_urls_to_intermediate_database(urls):
    logging.debug("In upload_urls_to_intermediate_database")
    intermediate_db = db_connections.get_intermediate_db()
    intermediate_db.begin()
    for url in urls:
        intermediate_db['urls_to_scrape'].upsert({'url': url}, ['url'], ensure=False)
    intermediate_db.commit()
    logging.debug("Bottom of  upload_urls_to_intermediate_database")


def scrape_location(page_source):
    tree = lxml.html.fromstring(page_source)
    locations = dict()
    location_sections = tree.xpath('//div[@class="project-location"]/a')

    # this comes from the website
    LOCATION_KEYS = ['name', 'short_name', 'country', 'id', 'is_root',
                     'state', 'urls', 'type', 'displayable_name', 'slug']
    for l_section in location_sections:
        location = json.loads(s=l_section.attrib['data-location'])
        if location.keys() != LOCATION_KEYS:
            logging.debug(location.keys())
            raise Exception("Bad location")
        # location['status'] = location['state']
        # del location['state']
        del location['urls']
        name = location['displayable_name'].strip()
        locations[name] = location
    db = db_connections.get_fungrosencrantz_schema(schema='kickstarter', traditional=True)
    db_connections.uploadOutputFile(data=locations.values(), db=db, table='location', strict=True)


def add_main_page_projects_to_intermediate_database():
    logging.debug("Getting html from main page")
    page_source = get_main_page_html()
    with open('main_page_source.pickle', 'wb') as f:
        pickle.dump(page_source, f)
    # with open('main_page_source.pickle') as f:
    #    page_source = pickle.load(f)
    logging.debug("Scraping for locations")
    scrape_location(page_source)
    logging.debug("Scraping for urls")
    urls = get_project_urls_from_main_page_html(page_source)
    logging.debug("Before upload_urls_to_intermediate_database")
    upload_urls_to_intermediate_database(urls)
    logging.debug("After upload_urls_to_intermediate_database")


if __name__ == "__main__":
    # get_main_page_html()
    add_main_page_projects_to_intermediate_database()
