from selenium import webdriver
from pyvirtualdisplay import Display
from lxml import html
import tablib
import json
import time
from asyncProjectScraper import openUrlFile
import pprint
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
import os
import requests
import db_connections


def downloadFile(url, folder=""):
    r = requests.get(url)
    file_name = url.split('/')[-1]
    if folder == "":
        file_path = file_name
    elif folder[-1] == '/':
        file_path = folder + file_name
    else:
        file_path = folder = "/" + file_name
    with open(file_path, 'wb') as f:
        f.write(r.content)


def get_file_name_from_url(url):
    return url.split('/')[-1]


def create_fast_firefox_profile():
    quickjava_url = 'https://addons.mozilla.org/firefox/downloads/latest/1237/addon-1237-latest.xpi'
    if not os.path.isfile(get_file_name_from_url(quickjava_url)):
        # download the extension
        downloadFile(quickjava_url)
    ## get the Firefox profile object
    firefox_profile = FirefoxProfile()
    ## Disable CSS
    firefox_profile.set_preference('permissions.default.stylesheet', 2)
    ## Disable images
    firefox_profile.set_preference('permissions.default.image', 2)
    ## Disable Flash
    firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so',
                                   'false')

    firefox_profile.add_extension(get_file_name_from_url(quickjava_url))
    firefox_profile.set_preference("thatoneguydotnet.QuickJava.curVersion", "2.0.6.1")  ## Prevents loading the 'thank you for installing screen'
    firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.Images", 2)  ## Turns images off
    firefox_profile.set_preference("thatoneguydotnet.QuickJava.startupStatus.AnimatedImage", 2)  ## Turns animated images off


def get_backers_tree(url, maxWait=60, waitStep=0.3):
    """
    url is the URL for the home tab of a kickstarter campaign page
    This function returns a list of backer urls for the kickstarter campaign.
    """
    backers_url = url + '/backers'
    """resp = requests.get(backers_url)
    backers = []
    # Make sure url is live, return empty list if not
    sc = resp.status_code
    if sc != 200:
        print "Got status code {0} for backers tab of url {1}".format(sc, url)
        return backers
    """
    try:
        # Create virtual display
        display = Display(visible=False, size=(800, 600))
        display.start()

        # Use selenium to load content
        driver = webdriver.Firefox()

        driver.get(backers_url)
        ps = ''
        total_wait_time = 0
        # while len(ps) != len(driver.page_source):
        while total_wait_time < maxWait:
            ps = driver.page_source
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(waitStep)  # need to add delay, but play with the pause time
            total_wait_time += waitStep
            backers_tree = html.fromstring(ps)
            backer_pages = backers_tree.xpath('//li[@class="page"]')
            if backer_pages[-1].attrib['data-last_page'] == "true":
                break
        display.stop()
        backers_tree = html.fromstring(ps).xpath('//div[contains(@class, "NS_projects__content")]')[0]
    except:
        raise
        backers_tree = None
    return backers_tree


def get_all_backers_as_html(urls, verbose=False):
    backers = []
    for i, url in enumerate(urls):
        tree = get_backers_tree(url)
        text = html.tostring(tree)
        backers.append(text)
        if verbose:
            print
            "Done with {0} out of {1}".format(i + 1, len(urls))

    return backers


def urls_file_to_json(input_file='mainPageUrls.txt', output_file='backersPages.json'):
    urls = openUrlFile(input_file)
    htmls = get_all_backers_as_html(urls, verbose=False)
    with open(output_file, 'wb') as f:
        json.dump(zip(urls, htmls), f)


def get_projects_with_null_backerid(offset=0, limit=1000, project_table='project', backer_table='backer'):
    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    results = db.query("""SELECT id, url FROM {0} left join backer on {0}.id = {1}.projectid
                            where projectid is null and backers_count > 0
                            limit {2}, {3}""".format(project_table, backer_table, offset, limit))
    return [x for x in results]


# columns of backer_table must be in the following order:
# projectid, userid, name
# Exact name does not matter. Eg: projectid, id, username would be acceptable
def upload_scraped_projects(scraped_projects, backer_table='backer', schema='kickstarter'):
    db = db_connections.get_fungrosencrantz_schema(schema)
    data = tablib.Dataset()
    data.headers = tuple(db[backer_table].columns)
    for i in range(len(projects)):
        row = {data.headers[0]: projects[i][0],
               data.headers[1]: projects[i][1],
               data.headers[2]: projects[i][2]}
        data.append(row)
    return data


if __name__ == "__main__":
    projects = get_projects_with_null_backerid(offset=0, limit=10)
    asdf(projects)
