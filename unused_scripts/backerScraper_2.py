import splinter
import time
import tablib
import asyncProjectScraper
import os
import multiprocessing
import requests
import db_connections
import random


class Backer():
    def __init__(self, id=None, raw_location=None, name=None):
        self.id = id
        self.raw_location = raw_location
        self.name = name


def get_file_name_from_url(url):
    return url.split('/')[-1]


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


# takes projects of the form [id, url] from the queue and updates the backers db
def update_backers_db_from_queue(project_queue, fail_queue, process_id):
    ff_profile = os.getcwd() + "\\lib\\quick_firefox"
    browser = splinter.Browser('firefox', profile=ff_profile)
    while True:
        p = project_queue.get()
        if p is None:  # signal that there are no more projects
            browser.quit()
            project_queue.task_done()
            break
        url = p['url']
        projectid = p['id']
        try:
            backers = get_backers_from_url(url=url, max_wait=30, browser=browser)
            data = tablib.Dataset()
            data.headers = ['projectid', 'userid', 'name', 'raw_location']
            for backer in backers:
                row = (projectid, backer['id'], backer['name'], backer['raw_location'])
                data.append(row)
            db = db_connections.get_fungrosencrantz_schema(schema='kickstarter')
            db_connections.uploadOutputFile(data=data, db=db, table='backer')
            del db
            print
            "Success: " + url
        except:
            fail_queue.put(url)
            print
            "Failed: " + url
        project_queue.task_done()


# takes urls from the project db and adds them to the queue
def add_projects_to_queue(project_queue, num_processes):
    print
    "Getting projects..."
    projects = [x for x in get_projects_with_null_backerid(offset=0, limit=500)]
    print
    "Done getting {0} projects".format(len(projects))
    for project in projects:
        project_queue.put(project)

    # tell the processes using the queue that they can go home now
    for i in range(num_processes):
        project_queue.put(None)


def get_projects_with_null_backerid(offset=0, limit=1000, project_table='project', backer_table='backer'):
    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    q = """SELECT id, url FROM {0} left join backer on {0}.id = {1}.projectid
            where projectid is null and backers_count > 50
            order by backers_count asc""".format(project_table, backer_table)
    if limit is not None:
        q = q + "\nlimit {0}, {1}".format(offset, limit)
    return db.query(q)


# each backer is a dictionary with id, name, and raw_location
def get_backers_from_url(url, max_wait=10, wait_step=0.45, browser=None):
    backers = []
    if browser is None:
        quit_at_end = True
        browser = splinter.Browser('firefox')
    else:
        quit_at_end = False

    browser.visit(url + "/backers")

    numReloads = 0
    while browser.title.find("We're sorry, but something went wrong") != -1:
        time.sleep(random.random() * 3 + 0.1)
        browser.reload()
        numReloads += 1
        print
        "Oh no!"
        if numReloads > 5: break

    total_wait_time = 0
    scroll_height_toggle = 0  # sometimes not scrolling all the way down helps
    # try to scroll to the bottom until either it takes too long or it's the last page
    while True:
        ps = browser.html
        if scroll_height_toggle == 0:
            scroll_height_toggle = 3000
        else:
            scroll_height_toggle = 0
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight-{0});".format(scroll_height_toggle))
        time.sleep(wait_step)
        if ps != browser.html:  # loaded next page
            total_wait_time = 0
        else:  # same page
            total_wait_time += wait_step
        if total_wait_time >= max_wait:  # waited too long to load next page
            print
            "Took too long: " + url
            raise
        backer_pages = browser.find_by_xpath('//li[@class="page"]')
        if backer_pages[-1]['data-last_page'] == "true":  # got to the last page
            break

    # find all the backers on the page
    backer_sections = browser.find_by_xpath('//li[@class="page"]/div[@class="NS_backers__backing_row"]')
    for backer_section in backer_sections:
        person = dict()
        person['id'] = backer_section['data-cursor']
        info = backer_section.text.split('\n')
        person['name'] = info[0]
        if len(info) == 2:
            person['raw_location'] = info[1]  # make sure there's a location listed
        else:
            person['raw_location'] = None
        backers.append(person)

    if quit_at_end: browser.quit()
    return backers


def scrape():
    # kickstarter seems to allow 4 connections at once
    NUM_BROWSERS = 5
    projectUrls = asyncProjectScraper.openUrlFile('mainPageUrls.txt')
    project_queue = multiprocessing.JoinableQueue(maxsize=NUM_BROWSERS * 2)
    fail_queue = multiprocessing.queues.SimpleQueue()
    project_queue_writer = multiprocessing.Process(target=add_projects_to_queue, args=(project_queue, NUM_BROWSERS))
    project_queue_writer.daemon = True
    project_queue_writer.start()

    project_queue_readers = []
    browsers = []
    for i in range(NUM_BROWSERS):
        process = multiprocessing.Process(
            target=update_backers_db_from_queue, args=(project_queue, fail_queue, i))
        process.daemon = True
        process.start()
        project_queue_readers.append(process)
    project_queue_writer.join()
    for p in project_queue_readers:
        p.join()
    project_queue.join()
    # return failed urls
    failed_urls = []
    while not fail_queue.empty():
        failed_urls.append(fail_queue.get())
    return failed_urls


if __name__ == "__main__":
    t1 = time.time()
    numTries = 0
    while True:
        failed_urls = scrape()
        numTries += 1
        if len(failed_urls) == 0:  # or numTries >= 2:
            break
        print
        "TRYING AGAIN"

    if len(failed_urls) > 0:
        with open('backer_fail_urls.txt', 'wb') as f:
            for url in failed_urls:
                f.write(url + "\n")

    print
    time.time() - t1
