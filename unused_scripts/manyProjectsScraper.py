# runs many asyncProjectScraper many times
# puts each output in their own folder "/chunk_XX/"
# use combiner.py to combine all the chunks into one,
# as if asyncProjectScraper had just been run one the
# one large file

possibleArguments = """
/m scrape the main page
/f run through failed urls instead of split urls
/s split
/r run - scrape split files (implies /s)
/c combine scrapes
/start (index) starts scraping at index"""

import os
import time
import sys
import tablib
from mainPageScraper import scrapeMainPage

# from requests_futures.sessions import FuturesSession
# from concurrent.futures import ThreadPoolExecutor

URLS_AT_A_TIME = 260
TMP_DIR = "chunks_directory"
CHUNK_DIR_NAME = "chunk_"  # eg chunk_0, chunk_1, chunk_2, etc.
TMP_FILE_NAME = "urls_chunk.txt"
# URLS_FILE = "success_project_urls.txt"
ORIG_DIR = os.getcwd()
START_INDEX = 0
LAST_INDEX = None
HEADERS = ('projectid', 'description', 'title', 'url', 'goal', 'amount_required', 'start_date', 'end_date',
           'state_changed_at', 'state', 'category', 'location', 'currency', 'backers_count')


def chunks(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def openUrlFile(filename):
    urls = []
    with open(filename, "r") as f:
        for line in f:
            urls.append(line.replace('\n', ""))
    return urls


def writeUrlFile(filename, urls):
    with open(filename, "w") as text_file:
        for url in urls:
            text_file.write(url + '\n')


def createDir(path):
    if os.path.isdir(path): return
    try:
        os.makedirs(path)
    except OSError:
        time.sleep(.5)  # race condition between makedirs and isdir
        if not os.path.isdir(path):
            raise


def getCurrentDir(index):
    return ORIG_DIR + "\\" + TMP_DIR + "\\" + CHUNK_DIR_NAME + str(index)


def splitFile(filename):
    allUrls = openUrlFile(filename)

    urlChunks = chunks(allUrls, URLS_AT_A_TIME)

    createDir(TMP_DIR)

    for i, chunk in enumerate(urlChunks):
        createDir(getCurrentDir(i))
        writeUrlFile(getCurrentDir(i) + "\\" + TMP_FILE_NAME, chunk)


def scrapeCurDirectory():
    if not os.path.isfile(TMP_FILE_NAME):
        print
        "TRYING TO SCRAPE NONEXISTANT FILE"
        return
    sys.argv = ['asyncProjectScraper.py', TMP_FILE_NAME]  # pass the file to process
    d = dict(locals(), **globals())
    execfile(ORIG_DIR + "/asyncProjectScraper.py", d, d)  # scrape urls in passed file


# scrapes the files splitFile() has created
def scrape():
    origDir = os.getcwd()
    i = START_INDEX
    while True:
        if (LAST_INDEX is not None) and (i >= LAST_INDEX):
            break
        try:
            os.chdir(getCurrentDir(i))
        except:
            break  # directory not found?
        scrapeCurDirectory()
        i = i + 1
    os.chdir(origDir)


def combineScrapes():
    os.chdir(ORIG_DIR)
    data = tablib.Dataset()
    data.headers = HEADERS
    totalTsv = ""

    i = START_INDEX
    while True:
        if (LAST_INDEX is not None) and (i >= LAST_INDEX):
            break
        tmpData = tablib.Dataset()
        tmpData.headers = HEADERS
        if not os.path.isdir(getCurrentDir(i)):
            break
        with open(getCurrentDir(i) + "\\output.tsv", "rb") as f:
            tmpData.tsv = f.read()
        if i != 0:  # include headers the first time
            tmpData.headers = ""
        totalTsv = totalTsv + tmpData.tsv
        del tmpData
        i = i + 1
    data.tsv = totalTsv

    with open('output.tsv', 'wb') as f:
        f.write(data.tsv)


if __name__ == "__main__":
    arguments = sys.argv
    # /f scrape failed urls instead of split urls
    # /s split
    # /r run - scrape split files (implies /s)
    # /c combine scrapes
    if "/start" in arguments:
        START_INDEX = int(arguments[arguments.index("/start") + 1])
        print
        "Starting at index", START_INDEX
    try:
        URLS_FILE = arguments[1]
    except:
        URLS_FILE = ""
    if "/m" in arguments:
        scrapeMainPage()
        splitFile("mainPageUrls.txt")
        scrape()
        combineScrapes()
        sys.exit()
    if not os.path.isfile(URLS_FILE):
        print
        "File must be first argument."
        print
        possibleArguments
        sys.exit()
    if "/f" in arguments:
        TMP_FILE_NAME = "failedUrls.txt"
        scrape()
    if "/r" in arguments:
        splitFile(URLS_FILE)
        scrape()
    if "/s" in arguments:
        splitFile(URLS_FILE)
    if "/c" in arguments:
        combineScrapes()
