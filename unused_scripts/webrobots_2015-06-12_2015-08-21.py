""" This script takes the json file downloaded from webrobots.io and outputs two json files: 
locations.json: a list of dictionaries. Each dictionary is a row in the location table of the Kickstarter db
urls_for_ids: 	a dictionary where the key is a project id and the value is the url
WARNING: This script should be run with a 64 bit version of python27! It will run out of memory otherwise."""

import json
from kickstarter_updater import find_values_not_in_table, clean_url
import db_connections

file_name = 'Kickstarter_2015-06-12.json'

# fix json from webrobots
with open(file_name) as f:
    text = f.read()
text = text.replace('}\n{', '},\n{')
'''with open('fixed_json/{}'.format(file_name), 'w') as f:
    f.write(text)

with open('fixed_json/{}'.format(file_name), 'rb') as f:
    f_json = json.load(f)
'''
f_json = json.loads(text)

urls_for_ids = dict()
locations = []
location_ids = set()
for project in f_json:
    for main_page in project['projects']:
        url = main_page['urls']['web']['project']
        url = url.replace('http://www.kickstarter.com', 'https://www.kickstarter.com')
        id = main_page['id']
        urls_for_ids[id] = url
        try:
            location = main_page['location']
            id = location['id']
            if id not in location_ids:
                location_ids.add(id)
                url = location['urls']['web']['location']
                url = url.replace('http://www.kickstarter.com', 'https://www.kickstarter.com')
                slug = url.replace("https://www.kickstarter.com/locations/", "")
                if location['slug'] is not None and location['slug'] != slug:
                    print
                    location['slug'], slug
                location['slug'] = slug
                del location['urls']
                locations.append(location)
        except:
            pass

import os

os.chdir("../")

kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
intermediate_db = db_connections.get_intermediate_db()

# find projects with ids not already in the database
all_proj_ids = [{'id': int(x)} for x in urls_for_ids.keys()]
new_proj_ids = find_values_not_in_table(all_values=all_proj_ids, table='project', column='id', db=kickstarter_db)

# insert new projects into the intermediate database to download
urls_to_download = [{'url': clean_url(urls_for_ids[x])} for x in new_proj_ids]
db_connections.uploadOutputFile(urls_to_download, db=intermediate_db, table='urls_to_scrape', strict=False)

# put locations into the kickstarter database
id_set = set()
cleaned_locations = []
for i, loc in enumerate(locations):
    if loc['id'] not in id_set:
        cleaned_locations.append(loc)
        id_set.add(loc['id'])
db_connections.uploadOutputFile(cleaned_locations, db=kickstarter_db, table='location', strict=True)
