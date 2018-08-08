import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import logging
import time
import traceback
import os
import lxml.html
import cPickle as pickle
from unused_scripts import db_connections


def scrape_file(file_path):
    file_path = file_path.replace('\\', '/')
    if file_path[-1] == '/':
        file_path = file_path[:-1]
    url = 'https://' + file_path[file_path.index('www.kickstarter.com'):-len('/community')]
    with open(file_path) as f:
        tree = lxml.html.fromstring(f.read())

        backers_by_country = []
        backers_by_city = []
        new_backers = None
        repeat_backers = None
        error_message = None

        if '1001614212' in file_path:
            pass
        no_backers_element = tree.xpath('//h3[@class="normal mb3 mb7-sm"]/text()')
        if len(no_backers_element) > 0 and no_backers_element[0].strip() == "This project doesn't have any backers yet.":
            error_message = "No backers"
        elif len(no_backers_element) > 0 and "backed this project and the community is still growing!" in no_backers_element[0].strip():
            error_message = "Few backers"
        elif len(tree.xpath('//*[@id="purged_project"]')) > 0:
            error_message = "Purged"
        elif len(tree.xpath('//*[@id="hidden_project"]')) > 0:
            error_message = "Hidden"
        else:
            country_elements = tree.xpath('//div[@class="location-list js-locations-countries"]/div')
            city_elements = tree.xpath('//div[@class="location-list js-locations-cities"]/div')
            for country_element in country_elements:
                primary_location_address_element = country_element.xpath(
                    './/div[@class="primary-text js-location-primary-text"]/a')[0]
                address_elements = primary_location_address_element.attrib['href'].split('&')
                for address_part in address_elements:
                    if address_part.split('=')[0] == 'woe_id':
                        location_id = int(address_part.split('=')[1])
                backers_by_country.append(dict(
                    url=url,
                    primary_location=primary_location_address_element.text.strip(),
                    location_id=location_id,
                    num_backers=int(country_element.xpath(
                        './/div[@class="tertiary-text js-location-tertiary-text"]/text()')[0].strip()
                                    .replace(' backers', '').replace(' backer', '').replace(',', ''), )))
            for city_element in city_elements:
                primary_location_address_element = city_element.xpath(
                    './/div[@class="primary-text js-location-primary-text"]/a')[0]
                address_elements = primary_location_address_element.attrib['href'].split('&')
                for address_part in address_elements:
                    if address_part.split('=')[0] == 'woe_id':
                        location_id = int(address_part.split('=')[1])
                backers_by_city.append(dict(
                    url=url,
                    primary_location=primary_location_address_element.text.strip(),
                    location_id=location_id,
                    secondary_location=city_element.xpath(
                        './/div[@class="secondary-text js-location-secondary-text"]/a/text()')[0].strip(),
                    num_backers=int(city_element.xpath(
                        './/div[@class="tertiary-text js-location-tertiary-text"]/text()')[0].strip()
                                    .replace(' backers', '').replace(' backer', '').replace(',', ''), )))
            new_backers = int(
                tree.xpath('//div[@class="new-backers"]/div[@class="count"]/text()')[0].strip().replace(',', ''))
            repeat_backers = int(
                tree.xpath('//div[@class="existing-backers"]/div[@class="count"]/text()')[0].strip().replace(',', ''))

        return dict(new_backers=new_backers,
                    repeat_backers=repeat_backers,
                    backers_by_city=backers_by_city,
                    backers_by_country=backers_by_country,
                    error_message=error_message,
                    url=url)


def run(kickstarter_directory):
    last_file_index = 10000000000
    cur_file_index = 0
    failed_files = []
    community_data = []
    for root, dirs, files in os.walk(kickstarter_directory):
        for name in files:
            file_path = os.path.join(root, name)
            if name == 'community':
                try:
                    if cur_file_index >= last_file_index:
                        break
                    cur_file_index += 1
                    community_data.append(scrape_file(file_path))
                except:
                    raise
                    failed_files.append(file_path)
        if cur_file_index >= last_file_index:
            break

    with open('community_data.pickle', 'wb') as f:
        pickle.dump(community_data, f)
    with open('community_failed_files.pickle', 'wb') as f:
        pickle.dump(failed_files, f)


def upload_community_data(community_data=None):
    if community_data is None:
        with open('community_data.pickle', 'rb') as f:
            community_data = pickle.load(f)

    project_data = []
    backers_in_city_data = []
    backers_in_country_data = []
    for project in community_data:
        backers_in_city_data += project.pop('backers_by_city')
        backers_in_country_data += project.pop('backers_by_country')
        project_data.append(project)

    db = db_connections.get_fungrosencrantz_schema('kickstarter')
    # db_connections.uploadOutputFile(data=project_data, db=db, table='project')
    db_connections.uploadOutputFile(data=backers_in_city_data, db=db, table='backers_in_city')
    db_connections.uploadOutputFile(data=backers_in_country_data, db=db, table='backers_in_country')

    '''
    db.query('begin')
    for row in community_data:
        if row['new_backers'] is not None and row['repeat_backers'] is not None:
            q = 'update project set new_backers={}, repeat_backers={} where url="{}" and repeat_backers is null'.format(
                row['new_backers'], row['repeat_backers'], row['url'])
        elif row['new_backers'] is not None and row['repeat_backers'] is None:
            q = 'update project set new_backers={} where url="{}" and repeat_backers is null'.format(
                row['new_backers'], row['url'])
        elif row['new_backers'] is None and row['repeat_backers'] is not None:
            q = 'update project set repeat_backers={} where url="{}" and repeat_backers is null'.format(
                row['repeat_backers'], row['url'])
        else:
            continue
        db.query(q)
    db.query('commit;')
    '''


if __name__ == '__main__':
    t0 = time.time()
    log_file = '{}.log'.format(sys.argv[0][:-3])
    logging.basicConfig(level=logging.DEBUG, filename=log_file)  #
    logging.debug("Starting {}.".format(sys.argv[0]))
    try:
        # run('E:/kickstarter/www.kickstarter.com')
        upload_community_data()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        raise
    logging.info('{} has completed in {} seconds.'.format(sys.argv[0], time.time() - t0))
