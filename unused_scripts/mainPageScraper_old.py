import json
from lxml import html
import lxml
import requests
import codecs
from unused_scripts import db_connections
import sys


def writeUrlFile(filename, urls):
    with open(filename, "w") as text_file:
        for url in urls:
            text_file.write(url + '\n')


def get_urls_from_main_page():
    # page = requests.get('https://www.kickstarter.com/projects/jpeteranetz/an-american-apocalypse-the-reckoning')
    page = requests.get('https://www.kickstarter.com/discover/advanced?sort=newest')
    tree = html.fromstring(page.text)

    urls = []

    for href in tree.xpath('//div[@class="project-thumbnail"]//@href'):
        url = "https://www.kickstarter.com" + href
        url = url.split('?')[0]
        urls.append(url)

    return urls


def get_main_page_html():
    import splinter
    import os
    import time

    ff_profile = os.getcwd() + "\\lib\\quick_firefox"
    browser = splinter.Browser('firefox', profile=ff_profile)

    for i in range(5):
        try:
            browser.visit('https://www.kickstarter.com/discover/advanced?sort=newest')
            break
        except:
            "Trying to load page again..."

    with codecs.open('output_before.html', 'wb', encoding='utf-8') as f:
        f.write(browser.html)

    num_reloads = 0
    while browser.title.find("We're sorry, but something went wrong") != -1:
        time.sleep(2)
        browser.reload()
        num_reloads += 1
        print
        "Oh no!"
        if num_reloads > 5: break

    # to make sure we don't load too many urls
    print
    "Getting projects in the big database"
    project_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    all_project_urls = set([x['url'] for x in project_db.query('select url from project')])
    print
    "Getting projects in the intermediate database"
    intermediate_db = db_connections.get_intermediate_db()
    urls_to_scrape = set([x['url'] for x in intermediate_db.query('select url from urls_to_scrape')])

    print
    "Starting download...."
    wait_time = 0
    wait_step = 0.05
    max_wait = 60
    max_tries = 3
    num_tries = 0
    num_hrefs = 0
    last_num_hrefs = 0
    extra_pages = None
    while True:
        orig_html_length = len(browser.html)
        button = browser.find_by_xpath('//div[@class="load_more"]/a[@role="button"]')[-1]
        button.click()
        time.sleep(wait_step)
        while True:
            project_hrefs = browser.find_by_xpath('//h6[@class="project-title"]/a')
            num_hrefs = len(project_hrefs)
            if num_hrefs != last_num_hrefs:
                break
            print
            "Waiting..."
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
        print
        "{0} projects loaded".format(num_hrefs)
        last_href = project_hrefs[-1]['href']
        last_href = last_href.replace("?ref=newest", "")
        if last_href in all_project_urls or last_href in urls_to_scrape:
            if extra_pages is None:
                extra_pages = 3
            else:
                extra_pages += -1
            if extra_pages == 0:
                break

    page_source = browser.html
    browser.quit()
    return page_source


def download_main_page(file_name='main_page_download.html'):
    page_source = get_main_page_html()
    with codecs.open(file_name, 'wb', encoding='utf-8') as f:
        f.write(page_source)


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
    intermediate_db = db_connections.get_intermediate_db()
    intermediate_db.begin()
    for url in urls:
        intermediate_db['urls_to_scrape'].upsert({'url': url}, ['url'], ensure=False)
    intermediate_db.commit()


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
            print
            location.keys()
            raise Exception("Bad location")
        location['status'] = location['state']
        del location['state']
        del location['urls']
        name = location['displayable_name']
        locations[name] = location
    db = db_connections.get_fungrosencrantz_schema(schema='kickstarter', traditional=True)
    db_connections.uploadOutputFile(data=locations.values(), db=db, table='location', strict=True)


def add_main_page_projects_to_intermediate_database():
    print
    "Getting html from main page"
    page_source = get_main_page_html()
    print
    "Scraping for locations"
    scrape_location(page_source)
    print
    "Scraping for urls"
    urls = get_project_urls_from_main_page_html(page_source)
    upload_urls_to_intermediate_database(urls)


def run():
    message = """
        /r downloads the main page and then uploads it to the `urls_to_scrape` table of the intermediate database
            note that this does not save to main_page_download.html
        /u uploads main_page_download.html to the `urls_to_scrape` table of the intermediate database
        /d downloads the main page to main_page_download.html - DO NOT USE: Bad encoding
    """
    # html_file_name = 'main_page_download.html'
    html_file_name = 'live_projects.htm'

    if len(sys.argv) == 2:
        if sys.argv[1] == "/r":  # directly upload page main page source to the intermediate database
            add_main_page_projects_to_intermediate_database()
        elif sys.argv[1] == "/u":  # upload main_page_download.html to intermediate database
            with open(html_file_name, 'rb') as f:
                page_source = f.read()
            page_source = page_source.decode('utf-8')
            print
            "Scraping for locations"
            scrape_location(page_source)
            print
            "Scraping for urls"
            urls = get_project_urls_from_main_page_html(page_source)
            upload_urls_to_intermediate_database(urls)
        elif sys.argv[1] == "/d":  # download main page to main_page_download.html
            page_source = get_main_page_html()
            with codecs.open(html_file_name, 'wb', encoding='utf-8') as f:
                f.write(page_source)
        else:  # bad input
            print
            message
    else:
        print
        message


if __name__ == "__main__":
    run()
