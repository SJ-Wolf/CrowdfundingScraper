import dataset
import manyProjectsScraper
import json
import sys


# needs to change once new database is implemented...
def getLiveProjectUrls(kickstarterDb, projectTableName="project"):
    urls = [x['url'] for x in kickstarterDb.query("""
        SELECT url FROM {0}
        where state='live';""".format(projectTableName))]
    return urls


if __name__ == "__main__":
    with open("lib/fungrosencrantz_login", 'r') as f:
        login = json.load(f)
    projectSchemaName = 'kickstarter'
    connectionString = 'mysql://' + login['username'] + ":" + login['password'] + "@" + login['hostname'] + "/" + projectSchemaName + "?charset=utf8"
    kickstarterDb = dataset.connect(connectionString, row_type=dict, engine_kwargs={'encoding': 'utf-8'})
    urls = getLiveProjectUrls(kickstarterDb)
    manyProjectsScraper.writeUrlFile('live_urls.txt', urls)
    sys.argv = ['manyProjectsScraper.py', 'live_urls.txt', '/r', '/c']
    execfile("manyProjectsScraper.py")
