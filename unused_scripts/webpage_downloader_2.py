# doesn't work. dunno why. something something queues

import multiprocessing
from numpy import random
import pprint
from lxml import html
import requests
from requests.exceptions import ReadTimeout


class Worker:
    def __init__(self, url_queue, html_queue, retry_codes=[429], timeout=5):
        self.url_queue = url_queue
        self.html_queue = html_queue
        self.retry_codes = retry_codes
        self.timeout = timeout
        self.kill = False

    def run(self):
        while not self.kill:
            url = self.url_queue.get()
            response = requests.get(url, timeout=self.timeout)
            code = response.status_code
            if code == 200:  # success
                self.html_queue.put(response.content)
            elif code in self.retry_codes:
                self.url_queue.put()
            else:  # bad code, just put that as the html
                self.html_queue.put(code)
            self.url_queue.task_done()


def _html_queue_reader(html_queue):
    pass


def _get_html_from_url_queue(url_queue, html_queue, retry_codes=[429], timeout=5, verbose_level=0):
    while not url_queue.empty():
        url = url_queue.get()
        print
        url
        if url is None:
            url_queue.task_done()
            break
        try:
            response = requests.get(url, timeout=timeout)
            code = response.status_code
            if code == 200:  # success
                html_queue.put(response.content)
            elif code in retry_codes:
                if verbose_level > 1: print
                "\t Redirected - retrying"
                url_queue.put(url)
            else:  # bad code, just put that as the html
                html_queue.put(code)
        except requests.exceptions.ConnectionError:
            if verbose_level > 0: print
            "\tRequests Timeout"
            url_queue.put(url)
        except ReadTimeout:
            if verbose_level > 0: print
            "\tTimeout"
            url_queue.put(url)
        url_queue.task_done()
    print
    "Thread done"


def get_html_from_urls(urls, max_workers=4, verbose_level=0):
    assert None not in urls  # this is the kill signal for threads and shouldn't be a url anyway
    url_queue = multiprocessing.JoinableQueue()
    html_queue = multiprocessing.JoinableQueue()

    workers = []
    for i in range(max_workers):
        workers.append(
            multiprocessing.Process(target=_get_html_from_url_queue, kwargs={
                'url_queue': url_queue,
                'html_queue': html_queue,
                'retry_codes': [429],
                'timeout': 5,
                'verbose_level': verbose_level
            })
        )
    for url in urls:
        url_queue.put(url)
    for w in workers:
        w.daemon = True
        w.start()
    url_queue.join()
    htmls = []
    while not html_queue.empty():
        htmls.append(html_queue.get())
    '''
     # kill the workers
    for i in range(max_workers):
        url_queue.put(None)

    # make sure they're dead'''
    for w in workers:
        w.join()

    print
    htmls[0]
    print
    len(htmls), len(urls)


if __name__ == "__main__":
    response = requests.get('http://www.dmoz.org')
    print
    response.status_code
    tree = html.fromstring(response.text)
    urls = []
    for a in tree.xpath('//a'):
        try:
            url = a.attrib['href']
            if url[0] == "/":
                urls.append('http://www.dmoz.org' + url)
        except:
            pass
    get_html_from_urls(urls[:4], max_workers=1, verbose_level=1243)
