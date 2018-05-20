import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import json

projects = []
for i in range(1, 141 + 1):
    with open('front_page_json/{}.json'.format(i)) as f:
        j = json.load(f)
        projects += j
        break

urls = []

for project in projects:
    base_url = project['url'].split('/')[2]
    urls.append('https://www.indiegogo.com/projects/{}'.format(base_url))
    for page in ('description', 'updates', 'comments', 'pledges', 'gallery'):
        urls.append('https://www.indiegogo.com/private_api/campaigns/{}/{}'.format(base_url, page))

with open('sample_urls_to_scrape.txt', 'w') as f:
    for url in urls:
        f.write(str(url) + '\n')
