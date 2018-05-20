import dataset
import tablib
from requests_futures.sessions import FuturesSession
from lxml import html
import time
import asyncProjectScraper


class ProjectRewardScraper():
    pass


def getRewardUrl(baseUrl):
    if baseUrl[-1] == "/":
        return baseUrl + "rewards"
    else:
        return baseUrl + "/rewards"


# modified scrapeUrls in asyncProjectScraper
def scrapeUrls(urls, data, workers=30):
    session = FuturesSession(max_workers=workers)

    pages = []
    for i, url in enumerate(urls):
        pages.append(session.get(getRewardUrl(url)))

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
        rewardPage = responses[i]
        rewardTree = html.fromstring(rewardPage.text)
        if asyncProjectScraper.checkOverloadedSite(rewardTree):
            skippedUrls.append(url)
            continue
        rewards = scrapeProjectRewards(tree=rewardTree)
        headers = ['projectid', 'amount_required', 'backers_limit', 'description', 'backers_count', 'delivery', 'sp_conditions_domestic',
                   'sp_conditions_international']
        try:
            pVars = vars(p)
            if pVars['state'] == "404":
                pass
            else:
                data.append([pVars[key] for key in headers])
        except:
            print
            "Error appending"
    return skippedUrls


def getRewardTree(urls, workers=30):
    session = FuturesSession(max_workers=workers)

    pages = []
    for i, url in enumerate(urls):
        pages.append(session.get(getRewardUrl(url)))

    responses = []
    for p in pages:
        try:
            result = p.result()
        except:
            time.sleep(30)
            result = p.result()
        responses.append(result)
