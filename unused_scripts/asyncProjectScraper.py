# python -m pip install requests-futures
# python -m pip install requests
# python -m pip install tablib
# python -m pip install lxml

# don't forget: python -m pip install --upgrade pip
# to run, python asyncProjectScraper.py Kickstarter_2015-04-02.txt
# Can only run about a couple thousand websites before running out of memory!!!

from requests_futures.sessions import FuturesSession
from lxml import html
import tablib
from concurrent.futures import ThreadPoolExecutor
import sys
import time
import datetime

WORKERS = 5
# DEFAULT_FILE    = 'mainPageUrls.txt'
DEFAULT_FILE = 'C:\\Users\\kyle1\\Google Drive\\2015 Summer Internship\\Scrapy\\geoCoder\\src\\tmp.txt'
HEADERS = ('projectid', 'description', 'title', 'url', 'goal', 'amount_required', 'start_date', 'end_date',
           'state_changed_at', 'state', 'category', 'location', 'currency', 'backers_count')


class ProjectScraper:
    debugLevel = 0
    failedUrls = []

    @staticmethod
    def onlyNumerics(seq):
        return filter(type(seq).isdigit, seq.split(".")[0])

    # convert dates to the datetime format
    @staticmethod
    def stringToDate(s):
        s = s.split("T")[0].split("-")
        d = datetime.date(int(s[0]), int(s[1]), int(s[2]))
        return d

    def emptyProject(self):
        self.projectid = ""
        self.description = ""
        self.title = ""
        self.url = ""
        self.goal = ""
        self.amount_pledged = ""
        self.start_date = ""
        self.end_date = ""
        self.state_changed_at = ""
        self.state = ""
        self.category = ""
        self.location = ""
        self.currency = ""
        self.backers_count = ""

    def instantiateOtherProject(self):
        self.title = self.mainTree.xpath('//div[contains(@class, "NS_projects__header")]//a/text()')[0]

        # next to "Share this project" is the location and category
        locationSection = self.mainTree.xpath('//div[@id="project_share"]/..//b/text()')
        if len(locationSection) == 2:  # both location and category are given
            self.location = locationSection[0]
            self.category = locationSection[1]
        else:
            self.category = locationSection[0]
            self.location = ""

        # above "Share this project" is the description
        self.description = self.mainTree.xpath('//div[@id="project_share"]/../../p/text()')[0].replace("\n", '')

        # top right of page goes backers, pledge, then duration

        # backers
        self.backers_count = self.mainTree.xpath('//div[@id="backers_count"]')[0].attrib['data-backers-count']

        # pledge section
        self.amount_pledged = ProjectScraper.onlyNumerics(self.mainTree.xpath('//div[@id="pledged"]/data/text()')[0])
        self.goal = ProjectScraper.onlyNumerics(self.mainTree.xpath('//div[@id="pledged"]/../span/span[contains(@class, "money")]/text()')[0])

        # duration
        self.end_date = self.mainTree.xpath('//span[@id="project_duration_data"]')[0].attrib['data-end_time']
        self.duration = ProjectScraper.onlyNumerics(self.mainTree.xpath('//span[@id="project_duration_data"]')[0].attrib['data-duration'])

    def instantiateSuccessfulProject(self):
        self.title = self.mainTree.xpath('//div[contains(@class, "NS_project_profile__title")]//a/text()')[0]
        self.description = self.mainTree.xpath('//div[@class="NS_project_profiles__blurb"]/div/span/span/text()')[0].replace("\n", '')
        self.backers_count = ProjectScraper.onlyNumerics(self.mainTree.xpath('//div[@class="NS_projects__spotlight_stats"]/b/text()')[0])

        # description tree
        self.amount_pledged = ProjectScraper.onlyNumerics(self.descriptionTree.xpath("//span[contains(@class, 'money')]/text()")[0])
        self.goal = ProjectScraper.onlyNumerics(self.descriptionTree.xpath("//span[contains(@class, 'money')]/text()")[1])
        self.end_date = self.descriptionTree.xpath('//div[@class="NS_projects__funding_period"]//time')[1].attrib['datetime']

        locationSection = self.descriptionTree.xpath('//div[@class="row"]/div/div/a/b/text()')
        if len(locationSection) == 2:  # both location and category are given
            self.location = locationSection[0]
            self.category = locationSection[1]
        else:
            self.category = locationSection[0]
            self.location = ""

    def __init__(self, url, mainTree, descriptionTree, updateTree):
        self.mainTree = mainTree
        self.descriptionTree = descriptionTree
        self.updateTree = updateTree
        self.emptyProject()
        try:

            # and make sure the project still exists (check for a 404 error)
            try:
                # if this isn't found then it will trigger an exception
                text = self.mainTree.xpath('//h1/text()')[0]
                if text == "404":
                    self.state = "404"
                    self.url = url
                    return
            except:
                pass

            self.url = url
            # determine project status since this determines the layout - live, successful, unsuccessful
            # example of mainContent: ['Project5971_cxt', 'Project-state-failed', 'Project-is_starred-', 'Project-ended-true']

            mainContent = self.mainTree.xpath('//div[@id="main_content"]')[0].attrib['class'].split(" ")
            self.projectid = ProjectScraper.onlyNumerics(mainContent[0])
            self.state = mainContent[1].replace("Project-state-", "")

            # make sure project is not in a copyright dispute
            try:
                # if this isn't found then it will trigger an exception
                text = self.mainTree.xpath('//div[@id="hidden_project"]//strong/text()')[0]
                assert ' is the subject of an intellectual property dispute and is currently unavailable.' in text
                text = text.replace(' is the subject of an intellectual property dispute and is currently unavailable.', "")
                self.state = 'copyright'
                self.title = text
                self.url = url
                return
            except:
                pass

            if self.state == "purged":
                self.description = ""
                self.title = ""
                self.goal = ""
                self.amount_pledged = ""
                self.start_date = ""
                self.end_date = ""
                self.state_changed_at = ""
                self.category = ""
                self.location = ""
                self.currency = ""
                self.backers_count = ""
                return

            self.currency = self.mainTree.xpath("//span[contains(@class, 'money')]")[0].attrib['class'].split(" ")[1].upper()

            self.start_date = self.updateTree.xpath("//div[contains(@class, 'timeline__divider--launched')]//time")[0].attrib['datetime']

            if self.state == "live":
                self.instantiateOtherProject()
            elif self.state == "failed":
                self.instantiateOtherProject()
            elif self.state == "canceled":
                self.instantiateOtherProject()
            elif self.state == "suspended":
                # self.instantiateSuspendedProject()
                self.instantiateOtherProject()
            else:
                self.instantiateSuccessfulProject()

            self.amount_pledged = int(self.amount_pledged)
            self.backers_count = int(self.backers_count)
            self.category = self.category.encode('utf-8')
            self.description = self.description.encode("utf-8")
            # self.duration = int(self.duration)
            self.end_date = ProjectScraper.stringToDate(self.end_date)
            self.goal = int(self.goal)
            self.location = self.location.encode("utf-8")
            self.title = self.title.encode("utf-8")
            self.projectid = int(self.projectid)
            self.start_date = ProjectScraper.stringToDate(self.start_date)

            if self.state == "canceled":
                # cancelDateText = self.mainTree.xpath('//div[contains(@class, "NS_projects__funding_bar")]/div/div/p/data/text()')[0]
                # cancelDate = time.strptime(cancelDateText, "on %B %d, %Y")
                cancelDateText = self.mainTree.xpath('//div[contains(@class, "NS_projects__funding_bar")]/div/div/p/data')[0].attrib['data-value'].replace('"',
                                                                                                                                                           '')
                # self.state_changed_at = datetime.date(cancelDate.tm_year, cancelDate.tm_mon, cancelDate.tm_mday)
                self.state_changed_at = ProjectScraper.stringToDate(cancelDateText)
            else:
                self.state_changed_at = self.end_date
        except:
            print
            "Project scrape failed"
            print
            vars(self)
            ProjectScraper.failedUrls.append(url)
            if ProjectScraper.debugLevel == 1:
                raise
            elif ProjectScraper.debugLevel == 2:
                with open(url.split("/")[-1] + "_Main.html", "w") as f:
                    f.write(html.tostring(self.mainTree).encode('utf-8'))
                with open(url.split("/")[-1] + "_Update.html", "w") as f:
                    f.write(html.tostring(self.updateTree).encode('utf-8'))
                with open(url.split("/")[-1] + "_Description.html", "w") as f:
                    f.write(html.tostring(self.descriptionTree).encode('utf-8'))
                raise


def openUrlFile(filename):
    urls = []
    with open(filename, "r") as f:
        for line in f:
            urls.append(line.replace('\n', "").replace('\r', ""))
    return urls


def writeUrlFile(filename, urls):
    with open(filename, "w") as text_file:
        for url in urls:
            text_file.write(url + '\n')


def checkOverloadedSite(tree):
    try:
        pageTitle = tree.xpath('//title/text()')[0]
        return pageTitle == "We're sorry, but something went wrong (500)"
    except:
        return False


def scrapeUrls(urls, data, workers=30):
    session = FuturesSession(executor=ThreadPoolExecutor(max_workers=workers))

    pages = []
    for i, url in enumerate(urls):
        pages.append(session.get(url))
        pages.append(session.get(url + "/updates"))
        pages.append(session.get(url + "/description"))

    responses = []
    for p in pages:
        try:
            result = p.result()
        except:
            time.sleep(30)
            result = p.result()
        responses.append(result)
    print
    "done with responses"
    skippedUrls = []
    for i, url in enumerate(urls):
        #### DEBUG #####
        if url == "https://www.kickstarter.com/projects/1732614332/nomads":
            pass

        mainPage = responses[i * 3]  # slow!
        updatePage = responses[i * 3 + 1]
        descriptionPage = responses[i * 3 + 2]
        mainTree = html.fromstring(mainPage.text)
        updateTree = html.fromstring(updatePage.text)
        descriptionTree = html.fromstring(descriptionPage.text)

        if checkOverloadedSite(mainTree) or checkOverloadedSite(updateTree) or checkOverloadedSite(descriptionTree):
            skippedUrls.append(url)
            continue
        p = ProjectScraper(url=url, mainTree=mainTree, descriptionTree=descriptionTree, updateTree=updateTree)

        try:
            pVars = vars(p)
            if pVars['state'] == "404":
                pass
            else:
                data.append([pVars[key] for key in HEADERS])
        except:
            print
            "Error appending"
    return skippedUrls


def scrapeAll(scraperFunction=scrapeUrls):
    programStartTime = time.time()

    if len(sys.argv) > 1:
        print
        sys.argv
        filename = sys.argv[1]
    else:
        filename = DEFAULT_FILE
        # filename = 'Kickstarter_2015-04-02.txt'
        # filename = 'bigFileTest.txt'
    urls = openUrlFile(filename)

    data = tablib.Dataset()
    data.headers = HEADERS
    firstTime = True
    while True:
        startTime = time.time()
        if firstTime:
            skippedUrls = scraperFunction(urls=urls, data=data, workers=WORKERS)
            urlsToGo = len(urls)
        else:
            urlsToGo = len(skippedUrls)
            skippedUrls = scraperFunction(urls=skippedUrls, data=data, workers=WORKERS)
            skippedUrls.reverse()  # maybe switching the order will help - can't hurt
            if urlsToGo - len(skippedUrls) == 0: time.sleep(2)  # didn't get any done - wait a bit
        endTime = time.time() - startTime
        print
        "Completed " + str(urlsToGo - len(skippedUrls)) + " in " + str(endTime) + ".\t" + str(len(skippedUrls)) + " left to go."
        firstTime = False
        if len(skippedUrls) == 0:
            break

    # writeUrlFile(filename="skippedUrls.txt", urls = skippedUrls)
    programEndTime = time.time()
    try:
        perSecond = float(len(urls)) / (programEndTime - programStartTime)
    except:
        perSecond = "undefined"
    print
    "FINISHED!"
    print
    "Completed", len(urls), "in", programEndTime - programStartTime, "for", perSecond, "per second."

    with open('output.tsv', "wb") as f:
        f.write(data.tsv)

    writeUrlFile(urls=ProjectScraper.failedUrls, filename="failedUrls.txt")


if __name__ == "__main__":
    scrapeAll()
