# -*- coding: UTF-8 -*-
import json
import sys
import re

if '../' not in sys.path:
    sys.path.insert(0, '../')

# updates the project, reward, and update tables of the kickstarter database from the urls in urls_to_scrape
import logging
import os
import time

import lxml
from lxml.html import soupparser

import comment_downloader
import db_connections
import mainPageScraper
import snapshot_creator
import webpageDownloader
import traceback
import useful_functions
from selenium.common.exceptions import WebDriverException


def onlyNumerics(seq):
    return filter(type(seq).isdigit, seq.split(".")[0])


# includes everything but the start_date and state_changed columns of the project table
# these are found with the UpdateScraper
# Does not handle purged or copyright projects
class ProjectScraper:
    @staticmethod
    def get_project_data(tree):
        current_project_pattern = re.compile(r'current_project = ".*";')

        current_project_str = None
        for script in tree.xpath('//script'):
            current_project_search = current_project_pattern.search(lxml.html.tostring(script))
            if current_project_search is not None:
                assert current_project_str is None
                current_project_str = current_project_search.group(0)[len('current_project = "'):-2]
        assert current_project_str is not None
        current_project_data = json.loads(
            lxml.html.tostring(lxml.html.fromstring(current_project_str.decode('string_escape')), method='text',
                               encoding='unicode'))
        return current_project_data

    def get_copyright_project_name(self):
        try:
            # if this isn't found then it will trigger an exception
            text = self.mainTree.xpath('//div[@id="hidden_project"]//strong')[0].text
            assert text.find(' is the subject of an intellectual property dispute and is currently unavailable.') != -1
            text = text.replace(' is the subject of an intellectual property dispute and is currently unavailable.', "")
            text = text.strip()
            return text
        except:
            return None

    def instantiateOtherProject(self):
        # self.dict['title'] = self.mainTree.xpath('//div[contains(@class, "NS_projects__header")]//a')[0].text.strip()

        possible_titles_set = set([])
        # self.dict['title'] = self.mainTree.xpath('/html/head/title')[0].text.replace(u'—Kickstarter', '').strip()

        raw_titles_set = set([lxml.html.tostring(x, method='text', encoding='unicode').strip() for x in
                              self.mainTree.xpath('//h2[@class="type-24 type-28-sm type-38-md navy-700 medium mb3"]')])
        assert len(raw_titles_set) == 1
        self.dict['title'] = list(raw_titles_set)[0]
        """
        # next to "Share this project" is the location and category
        locationSection = self.mainTree.xpath('//div[@id="project_share"]/..//b/text()')
        if len(locationSection) == 2: # both location and category are given
            self.dict['raw_location'] = locationSection[0]
            self.dict['category'] = locationSection[1]
            slug_href = self.mainTree.xpath('//div[@id="project_share"]/../a')[0].attrib['href']
            self.dict['location_slug'] = slug_href.replace("/discover/places/", "").split("?")[0]
        else:
            self.dict['category'] = locationSection[0]
            self.dict['raw_location'] = None
        """
        assert len(self.mainTree.xpath('//div[@id="video-section"]')) == 1

        # next to the video is the location and category
        # locationSection = self.mainTree.xpath('//div[@id="video-section"]/..//a[@class="grey-dark mr2 nowrap"]')
        # locationSection = self.mainTree.xpath(
        #    '//div[@class="f5 mb3 mb5-sm"]/div[contains(@class, "NS_projects__category_location")]/a')
        if len(self.mainTree.xpath('//*[contains(@class, "projects-we-love-badge")]')) > 0:
            num_locations_offset = 1
            self.dict['is_loved_project'] = True
        else:
            num_locations_offset = 0
            self.dict['is_loved_project'] = False

        raw_location_set = set([lxml.html.tostring(x, method='text', encoding='unicode').strip() for x in
                                self.mainTree.xpath(
                                    '//div[contains(@class, "NS_projects__badges")]//a[contains(@href, "/discover/places/")]')])
        assert len(raw_location_set) <= 1

        if len(raw_location_set) == 1:
            self.dict['raw_location'] = list(raw_location_set)[0]
        else:
            self.dict['raw_location'] = None

        location_slug_set = set(
            [x.attrib['href'].replace("/discover/places/", "").split("?")[0].strip() for x in self.mainTree.xpath(
                '//div[contains(@class, "NS_projects__badges")]//a[contains(@href, "/discover/places/")]')])
        assert len(location_slug_set) <= 1

        if len(location_slug_set) == 1:
            self.dict['location_slug'] = list(location_slug_set)[0]
        else:
            self.dict['location_slug'] = None

        raw_subcategory_set = set([lxml.html.tostring(x, method='text', encoding='unicode').strip() for x in
                                   self.mainTree.xpath(
                                       '//div[contains(@class, "NS_projects__badges")]//a[contains(@href, "/discover/categories/")]')])
        assert len(raw_subcategory_set) == 1
        self.dict['subcategory'] = list(raw_subcategory_set)[0]

        raw_category_href_set = set([x.attrib['href'] for x in self.mainTree.xpath(
            '//div[contains(@class, "NS_projects__badges")]//a[contains(@href, "/discover/categories/")]')])
        assert len(raw_category_href_set) == 1
        self.dict['category'] = list(raw_category_href_set)[0].split('/')[3].split("?")[0].replace("%20", " ").strip()

        '''
        if len(locationSection) == 2 + num_locations_offset:  # both location and category are given
            self.dict['raw_location'] = locationSection[0].xpath('./text()')[0].strip()
            self.dict['subcategory'] = locationSection[1].xpath('./text()')[0].strip()
            self.dict['category'] = locationSection[1].attrib['href'].split('/')[3].split("?")[0].replace("%20",
                                                                                                          " ").strip()
            slug_href = locationSection[0].attrib['href']
            self.dict['location_slug'] = slug_href.replace("/discover/places/", "").split("?")[0].strip()
        elif len(locationSection) == 1 + num_locations_offset:
            self.dict['subcategory'] = locationSection[0].xpath('./b/text()')[0].strip()
            self.dict['category'] = locationSection[0].attrib['href'].split('/')[3].split("?")[0].replace("%20",
                                                                                                          " ").strip()
            self.dict['raw_location'] = None
        else:
            raise Exception('Unexpected number of location sections')
        '''

        # below the video is the description
        possible_desc_sections = self.mainTree.xpath('//p[@class="type-14 type-18-md navy-600 mb0"]')
        assert len(possible_desc_sections) == 1
        self.dict['description'] = lxml.html.tostring(possible_desc_sections[0], method='text',
                                                      encoding='unicode').strip()

        # top right of page goes backers, pledge, then duration

        # backers
        # self.backers_count = self.mainTree.xpath('//div[@id="backers_count"]')[0].attrib['data-backers-count']

        # pledge section
        # self.amount_pledged = onlyNumerics(self.mainTree.xpath('//div[@id="pledged"]/data')[0].text)
        self.dict['goal'] = onlyNumerics(
            self.mainTree.xpath('//div[@id="pledged"]/../span/span[contains(@class, "money")]')[0].text)

        # duration
        self.dict['end_date'] = self.mainTree.xpath('//span[@id="project_duration_data"]')[0].attrib['data-end_time']
        self.dict['end_date'] = self.dict['end_date'].split("T")[0].replace('"', "").strip()

        # self.dict['duration'] = onlyNumerics(self.mainTree.xpath('//span[@id="project_duration_data"]')[0].attrib['data-duration'])

        video_section = self.mainTree.xpath('//div[@id="video-section"]')[0]
        self.dict['has_video'] = (video_section.attrib['data-has-video'] == 'true')

    def instantiate_successful_project(self):
        self.dict['title'] = self.mainTree.xpath('//div[contains(@class, "NS_project_profile__title")]//a')[
            0].text.strip()
        self.dict['description'] = self.mainTree.xpath('//div[@class="NS_project_profiles__blurb"]/div/span/span')[
            0].text.replace("\n", '').strip()
        self.dict['backer_count'] = onlyNumerics(
            self.mainTree.xpath('//div[@class="NS_campaigns__spotlight_stats"]/b')[0].text.strip())

        # description tree
        self.dict['amount_pledged'] = onlyNumerics(
            self.descriptionTree.xpath("//span[contains(@class, 'money')]/text()")[0].strip())
        self.dict['goal'] = onlyNumerics(
            self.descriptionTree.xpath("//span[contains(@class, 'money')]/text()")[1].strip())
        self.dict['end_date'] = \
            self.descriptionTree.xpath('//div[@class="NS_campaigns__funding_period"]//time')[1].attrib['datetime']
        self.dict['end_date'] = self.dict['end_date'].split("T")[0].replace('"', "").strip()

        locationSection = self.descriptionTree.xpath('//div[@class="NS_projects__category_location ratio-16-9"]/a')

        if len(self.descriptionTree.xpath('//*[contains(@class, "projects-we-love-badge")]')) > 0:
            num_locations_offset = 1
            self.dict['is_loved_project'] = True
        else:
            num_locations_offset = 0
            self.dict['is_loved_project'] = False

        if len(locationSection) == 2 + num_locations_offset:  # both location and category are given
            self.dict['raw_location'] = locationSection[0].xpath('./text()')[0].strip()
            self.dict['subcategory'] = locationSection[1].xpath('./text()')[0].strip()
            self.dict['category'] = locationSection[1].attrib['href'].split('/')[3].split("?")[0].replace("%20",
                                                                                                          " ").strip()
            slug_href = locationSection[0].attrib['href']
            self.dict['location_slug'] = slug_href.replace("/discover/places/", "").split("?")[0].strip()
        elif len(locationSection) == 1 + num_locations_offset:
            self.dict['subcategory'] = locationSection[0].xpath('./text()')[0].strip()
            self.dict['category'] = locationSection[0].attrib['href'].split('/')[3].split("?")[0].replace("%20",
                                                                                                          " ").strip()
            self.dict['raw_location'] = None
        else:
            raise Exception('Unexpected number of location sections: {}'.format(len(locationSection)))

        try:
            video_section = self.descriptionTree.xpath('//div[@id="video-section"]')[0]
            self.dict['has_video'] = (video_section.attrib['data-has-video'] == 'true')
        except:
            self.dict['has_video'] = False

    def check_if_invalid(self):
        invalid_reasons = []
        if self.dict['id'] is None:
            invalid_reasons.append('Project: id is None')
        if len(invalid_reasons) == 0:
            return False
        else:
            return invalid_reasons

    def __init__(self, main_tree, description_tree):
        self.mainTree = main_tree
        self.descriptionTree = description_tree
        self.dict = dict()
        self.dict['id'] = None
        self.dict['title'] = None
        self.dict['description'] = None
        self.dict['url'] = None
        self.dict['goal'] = None
        self.dict['status'] = None
        self.dict['amount_pledged'] = None
        self.dict['start_date'] = None
        self.dict['end_date'] = None
        self.dict['status_changed'] = None
        self.dict['category'] = None
        self.dict['subcategory'] = None
        self.dict['currency'] = None
        self.dict['backer_count'] = None
        self.dict['has_video'] = None
        self.dict['body_length'] = None
        self.dict['body_image_count'] = None
        self.dict['body_video_count'] = None
        self.dict['comment_count'] = None
        self.dict['update_count'] = None
        self.dict['raw_location'] = None
        self.dict['founder_id'] = None
        self.dict['founder_name'] = None
        self.dict['location_slug'] = None
        self.dict['is_loved_project'] = None
        self.dict['about_this_project_html'] = None
        self.dict['new_backers'] = None
        self.dict['repeat_backers'] = None

        # example of mainContent: ['Project5971_cxt', 'Project-state-failed', 'Project-is_starred-', 'Project-ended-true']
        # mainContent = self.mainTree.xpath('//div[@id="main_content"]')[0].attrib['class'].split(" ")
        self.dict['status'] = self.mainTree.xpath('//*[@id="main_content"]')[0].attrib['class'].replace(
            'Campaign-state-', '')
        if self.dict['status'] != 'purged':
            self.dict['id'] = int(
                self.mainTree.xpath('//span[@class="count"]/data[contains(@class,"Project")]')[0].attrib[
                    'class'].replace(
                    'Project', ''))
            #        self.dict['id'] = onlyNumerics(mainContent[0].strip())
        else:
            current_project_pattern = re.compile(r'current_project = ".*";')

            current_project_str = None
            for script in self.mainTree.xpath('//script'):
                current_project_search = current_project_pattern.search(lxml.html.tostring(script))
                if current_project_search is not None:
                    assert current_project_str is None
                    current_project_str = current_project_search.group(0)[len('current_project = "'):-2]
            assert current_project_str is not None
            current_project_data = json.loads(
                lxml.html.tostring(lxml.html.fromstring(current_project_str[:-2]), method='text', encoding='unicode'))

        if self.dict['status'] == "purged":
            # self.convert_to_utf8()
            return
        # try to instantiate the project as if it were copyrighted; returns None if it's not copyrighted
        self.dict['title'] = self.get_copyright_project_name()
        if self.dict['title'] is not None:
            self.dict['status'] = "copyright"
            # self.convert_to_utf8()
            return

        try:
            self.dict['currency'] = \
                self.mainTree.xpath("//span[contains(@class, 'project_currency_code')]")[0].attrib['class'].split(' ')[
                    1].upper().strip()
        except IndexError:
            # format is CA$123,456
            raw_money_text = self.mainTree.xpath("//span[contains(@class, 'money')]")[
                0].text.upper().strip().replace(' ', '')
            currency_map_dict = dict()
            currency_map_dict[u'AU$'] = 'AUD'
            currency_map_dict[u'CA$'] = 'CAD'
            currency_map_dict[u'CHF'] = 'CHF'
            currency_map_dict[u'DKK'] = 'DKK'
            currency_map_dict[u'€'] = 'EUR'
            currency_map_dict[u'£'] = 'GBP'
            currency_map_dict[u'HK$'] = 'HKD'
            currency_map_dict[u'NOK'] = 'NOK'
            currency_map_dict[u'NZ$'] = 'NZD'
            currency_map_dict[u'SEK'] = 'SEK'
            currency_map_dict[u'S$'] = 'SGD'
            currency_map_dict[u'$'] = 'USD'

            for currency in currency_map_dict:
                if raw_money_text.startswith(currency):
                    assert self.dict['currency'] is None
                    original_currency_string = currency
                    self.dict['currency'] = currency_map_dict[currency]
            assert self.dict['currency'] is not None
        # money_string = int(raw_money_text[len(original_currency_string):].replace(',', ''))

        # find backers_count, amount_pledged, and comments_count
        # for live projects this only updates comment_count
        for d in self.mainTree.xpath('//data'):
            if d.attrib['itemprop'] == 'Project[backers_count]':
                self.dict['backer_count'] = d.attrib['data-value'].strip()
            elif d.attrib['itemprop'] == 'Project[pledged]':
                self.dict['amount_pledged'] = d.attrib['data-value'].strip()
                # self.dict['currency'] = d.attrib['data-currency']
            elif d.attrib['itemprop'] == 'Project[state_changed_at]':
                self.dict['status_changed'] = d.attrib['data-value'].split("T")[0].replace('"', "").strip()
            elif d.attrib['itemprop'] == 'Project[comments_count]':
                self.dict['comment_count'] = d.attrib['data-value'].strip()

        full_description_xpath = '//div[@class="NS_projects__description_section"]//div[@class="row"]' \
                                 + '//div[contains(@class, "full-description")]'
        self.dict['about_this_project_html'] = lxml.html.tostring(self.descriptionTree.xpath(
            '//div[@class="NS_projects__description_section"]//div[@class="row"]'
            + '/div[contains(@class, "description-container")]')[0])
        self.dict['body_length'] = 0
        full_description_texts = self.descriptionTree.xpath(full_description_xpath + '//p/text()')
        for text in full_description_texts:
            self.dict['body_length'] += len(text)

        self.dict['body_image_count'] = len(self.descriptionTree.xpath(
            full_description_xpath + '//img')
        )

        self.dict['body_video_count'] = len(self.descriptionTree.xpath(
            full_description_xpath + '//div[@class="template oembed"]')
        )

        # navigation_section = self.mainTree.xpath('//section[contains(@class, "js-project-nav")]')
        # assert len(navigation_section) == 1

        # previous way to find update text... they changed html
        # update_texts = self.mainTree.xpath('//a[@id="updates_nav"]/text()')
        # new way
        update_texts = self.mainTree.xpath('//a[@data-content="updates"]/span/text()')
        if len(update_texts) != 1:
            pass
        assert len(update_texts) == 1

        self.dict['update_count'] = onlyNumerics(update_texts[0].strip())

        creator_section = self.mainTree.xpath('//a[@data-modal-title="About the creator"]')
        for c in creator_section:
            try:
                self.dict['founder_name'] = c.text.strip()
                self.dict['founder_id'] = c.attrib['href'].split('/')[2].strip()
            except AttributeError:
                pass
            else:
                break
        assert self.dict['founder_name'] is not None
        assert self.dict['founder_id'] is not None

        if self.dict['status'] == 'successful':
            self.instantiate_successful_project()
        else:
            self.instantiateOtherProject()


class RewardScraper:
    def check_if_invalid(self):
        invalid_reasons = []
        for i in range(len(self.rewards)):
            for key in ('projectid', 'reward_number', 'backer_count'):
                if self.rewards[i].get(key) is None:
                    continue
                try:
                    float(self.rewards[i][key])
                except ValueError:
                    invalid_reasons.append('Reward {}: {} has invalid value: {}'.format(i, key, self.rewards[i][key]))
        if len(invalid_reasons) == 0:
            return False
        else:
            return invalid_reasons

    def __init__(self, reward_tree, projectid):
        self.projectid = projectid
        self.rewards = dict()
        self.rewards = []

        reward_sections = reward_tree.xpath('//div[@class="pledge__info"]')
        self.num_rewards = len(reward_sections)
        if self.num_rewards != 0:
            for i, reward in enumerate(reward_sections):
                self.rewards.append(dict())
                self.rewards[i]['projectid'] = self.projectid
                self.rewards[i]['reward_number'] = i
                reward_text = reward.xpath('./h2/span/text()')[0]
                self.rewards[i]['amount_required'] = int(onlyNumerics(reward_text))
                backer_text = reward.xpath('.//span[contains(@class,"pledge__backer-count")]/text()')[0]
                self.rewards[i]['backer_count'] = int(onlyNumerics(backer_text))
                self.rewards[i]['description'] = '\n'.join(reward.xpath('./div/p/text()')).strip()
                try:
                    backer_text = reward.xpath('.//span[@class="pledge__limit"]/text()')[0].strip()
                    self.rewards[i]['backer_limit'] = int(backer_text.split(' of')[1].split(')')[0].replace(',', ''))
                except:
                    self.rewards[i]['backer_limit'] = None
                self.rewards[i]['delivery'] = None
                self.rewards[i]['shipping_note'] = None
                for pledge_detail in reward.xpath('.//div[@class="pledge__detail"]'):
                    pledge_detail_text = pledge_detail.xpath('./span/text()')[0].strip()
                    if pledge_detail_text.lower() == "estimated delivery":
                        self.rewards[i]['delivery'] = pledge_detail.xpath('.//time')[0].attrib['datetime'].strip()
                    elif pledge_detail_text.lower() == "ships to":
                        self.rewards[i]['shipping_note'] = pledge_detail.xpath('./span')[1].text.strip()
                    else:
                        raise Exception("Unexpected pledge detail title: {}".format(pledge_detail_text))


class UpdateScraper:
    def add_reward_from_entry(self, entry):
        self.updates.append(dict())
        self.updates[-1]['projectid'] = self.projectid
        self.updates[-1]['post_date'] = entry.xpath('.//time')[0].attrib['datetime'].split('T')[0].strip()
        self.updates[-1]['title'] = entry.xpath('.//h2[@class="grid-post__title"]')[0].text.strip()

    def check_if_invalid(self):
        invalid_reasons = []
        for i in range(len(self.updates)):
            for key in ('projectid', 'update_number'):
                if self.updates[i].get(key) is None:
                    continue
                try:
                    float(self.updates[i][key])
                except ValueError:
                    invalid_reasons.append('Update {}: {} has invalid value: {}'.format(i, key, self.updates[i][key]))
        if len(invalid_reasons) == 0:
            return False
        else:
            return invalid_reasons

    def __init__(self, update_tree, project):
        self.project = project
        self.updates = []
        for i, entry in enumerate(update_tree.xpath('//div[@class="timeline"]/div')):
            entry_class = entry.attrib['class']
            split_entry_class = entry_class.split(" ")
            if "timeline__divider--month" in split_entry_class:
                pass
            elif entry_class == "timeline__divider":
                pass
            elif "timeline__divider--cancellation" in split_entry_class:
                self.updates.append(dict())
                self.updates[-1]['projectid'] = self.project.dict['id']
                self.updates[-1]['post_date'] = entry.xpath('.//time')[0].attrib['datetime'].split('T')[0].strip()
                self.updates[-1]['title'] = entry.xpath('.//div[@class="mb2"]/b')[0].text.strip()
                self.updates[-1]['update_number'] = i
            elif "timeline__divider--failure" in split_entry_class:
                self.updates.append(dict())
                self.updates[-1]['projectid'] = self.project.dict['id']
                self.updates[-1]['post_date'] = entry.xpath('.//time')[0].attrib['datetime'].split('T')[0].strip()
                self.updates[-1]['title'] = entry.xpath('.//div[@class="mb2"]/b')[0].text.strip()
                self.updates[-1]['update_number'] = i
            elif "timeline__divider--successful" in split_entry_class:
                self.updates.append(dict())
                self.updates[-1]['projectid'] = self.project.dict['id']
                self.updates[-1]['post_date'] = entry.xpath('.//time')[0].attrib['datetime'].split('T')[0]
                try:
                    self.updates[-1]['title'] = entry.xpath('.//div[@class="h3"]')[0].text.strip()
                except IndexError:
                    self.updates[-1]['title'] = 'Success'
                project.dict['start_date'] = self.updates[-1]['post_date']
                self.updates[-1]['update_number'] = i
                # if self.project.dict['status'] == "successful":
                #    project.dict['status_changed'] = self.updates[-1]['post_date']
            elif "timeline__divider--launched" in split_entry_class:
                self.updates.append(dict())
                self.updates[-1]['projectid'] = self.project.dict['id']
                self.updates[-1]['post_date'] = entry.xpath('.//time')[0].attrib['datetime'].split('T')[0]
                self.updates[-1]['title'] = entry.xpath('.//div[@class="f2"]')[0].text.strip()
                self.updates[-1]['update_number'] = i
                project.dict['start_date'] = self.updates[-1]['post_date']
            elif "timeline__item--right" in split_entry_class or "timeline__item--left" in split_entry_class:
                self.updates.append(dict())
                self.updates[-1]['projectid'] = self.project.dict['id']
                self.updates[-1]['post_date'] = entry.xpath('.//time')[0].attrib['datetime'].split('T')[0].strip()
                self.updates[-1]['title'] = entry.xpath('.//h2[@class="grid-post__title"]')[0].text.strip()
                self.updates[-1]['update_number'] = i
            elif "timeline__divider--potd" in split_entry_class:
                self.updates.append(dict())
                self.updates[-1]['projectid'] = self.project.dict['id']
                self.updates[-1]['post_date'] = entry.xpath('.//time')[0].attrib['datetime'].split('T')[0].strip()
                self.updates[-1]['title'] = entry.xpath('./div/b')[0].text.strip()
                self.updates[-1]['update_number'] = i

            else:
                raise Exception(
                    "Unknown reward entry: {0}\nLast url done: {1}".format(entry_class, self.project.dict['url']))

            if self.updates[-1]['title'] == "":
                raise Exception("No title: {0}".format(entry_class))


def check_purged_project(tree):
    try:
        tree.xpath('//div[@id="purged_project"]')[0]
        return False
    except:
        return True


def get_copyright_project_name(tree):
    try:
        # if this isn't found then it will trigger an exception
        text = tree.xpath('//div[@id="hidden_project"]//strong')[0].text
        assert ' is the subject of an intellectual property dispute and is currently unavailable.' in text
        text = text.replace(' is the subject of an intellectual property dispute and is currently unavailable.', "")
        text = text.strip()
        return text
    except:
        return None


def test_url(url):
    import requests
    main_tree = lxml.html.fromstring(requests.get(
        url).content)
    desc_tree = lxml.html.fromstring(requests.get(
        '{}/description'.format(url)).content)
    update_tree = lxml.html.fromstring(requests.get(
        '{}/updates'.format(url)).content)
    reward_tree = lxml.html.fromstring(requests.get(
        '{}/rewards'.format(url)).content)

    p = ProjectScraper(main_tree, desc_tree)
    r = RewardScraper(reward_tree, p.dict['id'])
    u = UpdateScraper(update_tree, p)


# returns urls from urls_to_scrape that aren't in the html tables
def get_urls_to_download(db=None, table_name=None):
    if db is None:
        db = db_connections.get_intermediate_db()
        logging.debug("db = {}".format(db))
    urls_to_download = set()
    if table_name is None:  # check urls against all tables
        tables = ("description_html", "main_html", "reward_html", "update_html")
        for t in tables:
            urls_to_download = urls_to_download.union(get_urls_to_download(db, t))
    else:
        results = db.query(
            'select urls_to_scrape.url, {0}.url as html_table_url from urls_to_scrape left join {0} on urls_to_scrape.url = {0}.url where html_table_url is null'.format(
                table_name))
        logging.debug("results = {}".format(results))
        for result in results:
            urls_to_download.add(result['url'])

    return urls_to_download


def update_html_tables(overwrite, workers=4, chunk_size=104, verbose_level=0):
    b_insert = not overwrite

    if not overwrite:
        urls = get_urls_to_download(table_name='reward_html')
    else:
        db = db_connections.get_intermediate_db()
        urls = [x['url'].split('?')[0] for x in db['urls_to_scrape'].all()]
    webpageDownloader.urls_to_database(base_urls=urls, db_connector=db_connections.get_intermediate_db,
                                       html_table_name='reward_html', url_append='/rewards',
                                       chunk_size=chunk_size, max_workers=workers, verbose_level=verbose_level,
                                       check_if_already_downloaded=False, b_insert=b_insert)
    if not overwrite:
        urls = get_urls_to_download(table_name='description_html')
    webpageDownloader.urls_to_database(base_urls=urls, db_connector=db_connections.get_intermediate_db,
                                       html_table_name='description_html', url_append='/description',
                                       chunk_size=chunk_size, max_workers=workers, verbose_level=verbose_level,
                                       check_if_already_downloaded=False, b_insert=b_insert)
    if not overwrite:
        urls = get_urls_to_download(table_name='main_html')
    webpageDownloader.urls_to_database(base_urls=urls, db_connector=db_connections.get_intermediate_db,
                                       html_table_name='main_html', url_append='',
                                       chunk_size=chunk_size, max_workers=workers, verbose_level=verbose_level,
                                       check_if_already_downloaded=False, b_insert=b_insert)
    if not overwrite:
        urls = get_urls_to_download(table_name='update_html')
    webpageDownloader.urls_to_database(base_urls=urls, db_connector=db_connections.get_intermediate_db,
                                       html_table_name='update_html', url_append='/updates',
                                       chunk_size=chunk_size, max_workers=workers, verbose_level=verbose_level,
                                       check_if_already_downloaded=False, b_insert=b_insert)


def get_data_unique_on_columns(data, columns=['id', 'name']):
    if len(columns) == 1:
        def get_key_repr(row):
            return row[columns[0]]
    else:
        def get_key_repr(row):
            key_repr = ""
            for key in columns:
                key_repr += repr(row[key])
            return key_repr

    id_set = set([get_key_repr(x) for x in data])
    if len(id_set) == len(data):
        return data
    used_ids = dict.fromkeys(id_set)
    new_data = list()
    for i in reversed(range(len(data))):
        cur_row = data[i]
        cur_id = get_key_repr(cur_row)

        if used_ids[cur_id] is None:
            used_ids[cur_id] = True
            new_data.append(data[i])
    return new_data


def scrape_intermediate_database(insert=True, skip_projects_in_kickstarter=False):
    def move_to_failed_urls_table(url, failure_reason, intermediate_db):
        if type(failure_reason) != str:
            failure_reason = str(failure_reason)
        intermediate_db['failed_urls'].upsert(dict(url=url, failure_reason=failure_reason), ensure=False, keys=['url'])
        intermediate_db.query('delete from urls_to_scrape where url = "{}"'.format(url))

    intermediate_db = db_connections.get_intermediate_db()
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    kickstarter_db.query("SET SESSION sql_mode = 'TRADITIONAL'")
    project_columns = set(kickstarter_db['project'].columns)
    update_columns = set(kickstarter_db['update'].columns)
    reward_columns = set(kickstarter_db['reward'].columns)

    if skip_projects_in_kickstarter:
        all_urls = intermediate_db['urls_to_scrape'].all()
        kickstarter_db['tmp'].insert_many(all_urls)
        q = """select distinct tmp.url from tmp left join project
        on tmp.url = project.url
        where project.url is null;"""
        urls = set([x['url'] for x in kickstarter_db.query(q)])
        kickstarter_db.query('drop table tmp')
    else:
        urls = set([x['url'] for x in intermediate_db['urls_to_scrape']])
    tables = ('description_html', 'main_html', 'reward_html', 'update_html')
    projects_chunk = []
    rewards_chunk = []
    updates_chunk = []
    chunk_size = 1000
    num_failed_urls = 0
    for i, url in enumerate(urls):
        page_sources = dict()
        for table in tables:
            page_sources[table.replace("_html", '')] = intermediate_db[table].find_one(url=url)['html']

        # check for page errors
        if page_sources['main'] == '404':
            logging.log(logging.DEBUG, "Url has 404 error: " + url)
        elif len(page_sources['main']) == 3:  # other error
            logging.log(logging.WARNING, "Url has unknown error: " + url)
        else:
            # from bs4 import UnicodeDammit
            # doc = UnicodeDammit(page_sources['description'], is_html=True)
            # parser = lxml.html.HTMLParser(encoding=doc.original_encoding)
            main_tree = lxml.html.fromstring(page_sources['main'])
            desc_tree = lxml.html.document_fromstring(page_sources['description'])
            reward_tree = lxml.html.fromstring(page_sources['reward'])
            update_tree = lxml.html.fromstring(page_sources['update'])

            try:
                # In extremely rare cases, the html is bad and the soupparser must be used, which is slower
                try:
                    p = ProjectScraper(main_tree, desc_tree)
                except:
                    main_tree = soupparser.fromstring(page_sources['main'])
                    desc_tree = soupparser.fromstring(page_sources['description'])
                    p = ProjectScraper(main_tree, desc_tree)
            except:
                logging.debug("Url failed: " + url)
                raise
                intermediate_db['failed_urls'].upsert(row=dict(url=url), ensure=False, keys=['url'])
                num_failed_urls += 1
                if float(num_failed_urls) / (((len(urls) + 5 * i) / 6) + .01) > .1:  # heavily weight by current index
                    logging.debug("Last url done: " + url)
                    for table in page_sources.keys():
                        with open('{0}.html'.format(table), 'wb') as f:
                            f.write(page_sources[table].encode('utf-8'))
                    f = open('main_tree.html', 'wb')
                    f.write(lxml.html.tostring(main_tree))
                    f = open('desc_tree.html', 'wb')
                    f.write(lxml.html.tostring(desc_tree))
                    f.close()
                    raise Exception("Too many projects failed to parse")
                continue

            p.dict['url'] = url

            invalid_project_reasons = p.check_if_invalid()
            if invalid_project_reasons:
                move_to_failed_urls_table(url=url, failure_reason=invalid_project_reasons,
                                          intermediate_db=intermediate_db)
                continue

            # It might be possible for this html to be bad as well, so try to use the soup parser if scraping fails
            try:
                try:
                    r = RewardScraper(reward_tree, p.dict['id'])
                    u = UpdateScraper(update_tree, p)
                except:
                    reward_tree = soupparser.fromstring(page_sources['reward'])
                    update_tree = soupparser.fromstring(page_sources['update'])
                    r = RewardScraper(reward_tree, p.dict['id'])
                    u = UpdateScraper(update_tree, p)
            except:
                logging.debug("Url failed: " + url)
                intermediate_db['failed_urls'].upsert(row=dict(url=url, failed_reason='reward or update scrape failed')
                                                      , ensure=False, keys=['url'])
                num_failed_urls += 1
                if float(num_failed_urls) / ((len(urls) + 5 * i) / 6) > .1:  # heavily weight by current index
                    logging.debug("Last url done: " + url)
                    for table in page_sources.keys():
                        with open('{0}.html'.format(table), 'wb') as f:
                            f.write(page_sources[table].encode('utf-8'))
                    f = open('reward_tree.html', 'wb')
                    f.write(lxml.html.tostring(reward_tree))
                    f = open('update_tree.html', 'wb')
                    f.write(lxml.html.tostring(update_tree))
                    f.close()
                    raise Exception("Too many projects failed to parse")
                continue

            invalid_update_reasons = u.check_if_invalid()
            if invalid_update_reasons:
                move_to_failed_urls_table(url=url, failure_reason=invalid_update_reasons,
                                          intermediate_db=intermediate_db)
                continue
            invalid_reward_reasons = r.check_if_invalid()
            if invalid_reward_reasons:
                move_to_failed_urls_table(url=url, failure_reason=invalid_reward_reasons,
                                          intermediate_db=intermediate_db)
                continue

            try:
                if project_columns != set(p.dict.keys()):
                    logging.debug(project_columns)
                    logging.debug(set(p.dict.keys()))
                    logging.debug("project_columns - keys: {0}".format(project_columns - set(p.dict.keys())))
                    logging.debug("keys - set(p.dict.keys()): {0}".format(set(p.dict.keys()) - project_columns))
                    time.sleep(0.5)
                    raise Exception("project_columns != set(p.dict.keys()")
                for reward in r.rewards:
                    key_set = set(reward.keys())
                    if not reward_columns == key_set:
                        logging.debug(reward_columns)
                        logging.debug(reward.keys())
                        logging.debug(reward_columns - key_set)
                        logging.debug(key_set - reward_columns)
                        time.sleep(0.5)
                        raise Exception()
                for update in u.updates:
                    key_set = set(update.keys())
                    if not update_columns == key_set:
                        logging.debug(update_columns)
                        logging.debug(update.keys())
                        logging.debug(update_columns - key_set)
                        logging.debug(key_set - update_columns)
                        time.sleep(0.5)
                        raise Exception()
            except:
                logging.debug("Index {0}. Last url done: {1}".format(i, url))
                raise

            projects_chunk.append(p.dict)
            rewards_chunk += r.rewards
            updates_chunk += u.updates

        if (i > 0 and i % chunk_size == 0) or i == len(urls) - 1:
            logging.debug("Updating through {0} of {1}".format(i, len(urls)))
            t1 = time.time()
            projects_chunk = get_data_unique_on_columns(data=projects_chunk, columns=['id'])
            rewards_chunk = get_data_unique_on_columns(data=rewards_chunk, columns=['projectid', 'reward_number'])
            updates_chunk = get_data_unique_on_columns(data=updates_chunk, columns=['projectid', 'update_number'])
            num_tries = 0
            while True:
                try:
                    db_connections.uploadOutputFile(
                        data=projects_chunk, db=kickstarter_db, table='project',
                        update=not skip_projects_in_kickstarter)
                    db_connections.uploadOutputFile(
                        data=rewards_chunk, db=kickstarter_db, table='reward', update=not skip_projects_in_kickstarter)
                    db_connections.uploadOutputFile(
                        data=updates_chunk, db=kickstarter_db, table='update', update=not skip_projects_in_kickstarter)
                    # db['project'].insert_many(projects_chunk, ensure=False)
                    # db['reward'].insert_many(rewards_chunk, ensure=False)
                    # db['update'].insert_many(updates_chunk, ensure=False)
                    break
                except:
                    num_tries += 1
                    if num_tries >= 2:
                        raise

            # except:
            #    print "Error. Rolling back changes..."
            #    try:
            #        db.query("""CREATE temporary TABLE `delete_table` (
            #                    `projectid` int(11) NULL,
            #                    PRIMARY KEY (`projectid`)
            #                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""
            #                 )
            #        db['delete_table'].insert_many([{'projectid' : x['id']} for x in projects_chunk ], ensure=False)
            #        db.query("delete from kickstarter.`project` where id in (select projectid from delete_table) limit 999999")
            #        db.query("delete from kickstarter.`reward` where projectid in (select projectid from delete_table) limit 999999")
            #        db.query("delete from kickstarter.`update` where projectid in (select projectid from delete_table) limit 999999")
            #        print "Roll back successful"
            #    raise

            projects_chunk = []
            rewards_chunk = []
            updates_chunk = []
            logging.debug(time.time() - t1)
            # if we get this far, we can safely remove the url from urls_to_scrape
            # intermediate_db.query('delete from urls_to_scrape where url="{}"'.format(url))


def update_location_data_for_live_projects(project_name):
    project_name = project_name.replace(" ", "+")
    url = "https://www.kickstarter.com/projects/search?utf8=&term=" + project_name
    import webbrowser
    webbrowser.open_new(url)
    # html_responses, num_passes = webpageDownloader.get_html_from_urls(urls=[url], workers=1)
    # page_source = html_responses.values()[0]
    # mainPageScraper.scrape_location(page_source)


def add_live_projects_to_intermediate_database():
    intermediate_db = db_connections.get_intermediate_db()
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')

    result = kickstarter_db.query('select url from project where status = "live"')
    urls = [x for x in result]
    db_connections.uploadOutputFile(data=urls, db=intermediate_db, table='urls_to_scrape', strict=False)


def add_all_downloaded_projects_to_intermediate_database():
    intermediate_db = db_connections.get_intermediate_db()
    intermediate_db.begin()
    intermediate_db.query("insert into urls_to_scrape select url from main_html")
    intermediate_db.commit()


def run_complete_update():
    """
    Updates everything in the Kickstarter database except comments.
    :return: None
    """

    def print_time_elapsed(t1):
        logging.debug(time.time() - t1)

    intermediate_db = db_connections.get_intermediate_db()
    intermediate_db.query('delete from urls_to_scrape')
    t1 = time.time()
    add_all_downloaded_projects_to_intermediate_database()

    print_time_elapsed(t1)

    update_html_tables()

    print_time_elapsed(t1)

    scrape_intermediate_database(insert=False, skip_projects_in_kickstarter=False)

    print_time_elapsed(t1)

    intermediate_db = db_connections.get_intermediate_db()
    intermediate_db.query('delete from urls_to_scrape')

    print_time_elapsed(t1)


def update_kickstarter_from_intermediate_database(skip_duplicates=False):
    update_html_tables(overwrite=not skip_duplicates, verbose_level=2)
    scrape_intermediate_database(insert=False, skip_projects_in_kickstarter=skip_duplicates)


def rescrape_intermediate_database():
    db_connections.get_intermediate_db().query('delete from urls_to_scrape')
    add_all_downloaded_projects_to_intermediate_database()
    scrape_intermediate_database(insert=False, skip_projects_in_kickstarter=False)


def download_all_of_kickstarter_into_database(start_index=0, chunk_size=200):
    import dataset
    import cPickle as pickle
    import traceback

    # get all urls from kickstarter database
    int_db = db_connections.get_intermediate_db()

    if start_index == 0:
        # get all urls
        ks_db = db_connections.get_fungrosencrantz_schema(schema='kickstarter', traditional=True)
        urls = [x['url'] for x in ks_db.query('select url from project;')]
        with open('all_urls.pickle', 'wb') as f:
            pickle.dump(urls, f)

        # truncate every table
        for table in ('reward_html', 'description_html', 'main_html', 'urls_to_scrape', 'failed_urls'):
            q = """SELECT sql FROM sqlite_master WHERE type='table' AND name='{}';""".format(table)
            create_q = int_db.query(q).next()['sql']
            int_db.query('drop table {}'.format(table))
            int_db.query(create_q)
        update_html_tables(overwrite=False, verbose_level=2)
    else:
        with open('all_urls.pickle', 'rb') as f:
            urls = pickle.load(f)

    # add urls to be scraped
    while True:
        url_data = [dict(url=x) for x in urls[start_index:start_index + chunk_size]]
        if len(url_data) == 0:  # done
            break
        int_db.query('delete from urls_to_scrape')
        int_db['urls_to_scrape'].insert_many(url_data)
        for i in range(5):
            try:
                # update_kickstarter_from_intermediate_database(skip_duplicates=False)
                update_html_tables(overwrite=False)
                break
            except:
                logging.error(traceback.format_exc())
                logging.info('Waiting before retrying...')
                time.sleep(10 * 60)  # wait 10 minutes before trying again
                logging.info('Done waiting.')
                if i == 4:
                    logging.error('last starting index = {}')
                    raise
        start_index += chunk_size


def find_values_not_in_table(all_values, table, column, db):
    db['tmp'].insert_many(all_values)
    q = """select tmp.{0} from tmp left join {1}
    on tmp.{0} = {1}.{0}
    where {1}.{0} is null;""".format(column, table)
    new_values = set([x[column] for x in db.query(q)])
    db.query('drop table tmp')
    return new_values


def clean_url(url):
    index_of_ref = url.rfind('?ref')
    if index_of_ref != -1:
        return url[:index_of_ref]
    else:
        return url


def daily_update():
    intermediate_db = db_connections.get_intermediate_db()

    # scrape live projects again
    intermediate_db.query('delete from urls_to_scrape')
    add_live_projects_to_intermediate_database()
    update_kickstarter_from_intermediate_database(skip_duplicates=False)
    comment_downloader.update_comments(0)  # update comments on live projects

    # scrape new projects
    intermediate_db.query('delete from urls_to_scrape')
    mainPageScraper.add_main_page_projects_to_intermediate_database()
    update_kickstarter_from_intermediate_database(skip_duplicates=True)
    comment_downloader.update_comments(
        1)  # update comments for any project with no uploaded comments when there should be

    snapshot_creator.create_snapshot()


if __name__ == "__main__":
    # import logging
    # logging.basicConfig(filename='kickstarter_reupload.log', level=logging.DEBUG)
    # update_html_tables(overwrite=True, verbose_level=2)
    # add_all_downloaded_projects_to_intermediate_database()
    # scrape_intermediate_database(insert=False, skip_projects_in_kickstarter=False)
    # daily_update()
    '''
    import json
    from pprint import pprint
    with open('locations.json', 'rb') as f:
        location_data = json.load(f)
    print len(location_data)
    ids = set()
    slugs = set()
    for x in location_data:
        ids.add(x['id'])
        slugs.add(x['slug'])

    print len(ids)
    print len(slugs)'''

    # db = db_connections.get_fungrosencrantz_schema('kickstarter', traditional=True)

    # run_complete_update(download_projects=False)
    # scrape_intermediate_database(insert=False, skip_project  s_in_kickstarter=False)
    # daily_update()
    # update_html_tables(overwrite=False, verbose_level=2)
    # rescrape_intermediate_database()

    log_file = 'kickstarter_updater.log'
    if os.path.exists(log_file):
        os.remove(log_file)
    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    logging.debug("In daily update")
    try:
        # scrape_intermediate_database(insert=False, skip_projects_in_kickstarter=False)
        # daily_update()
        # scrape_intermediate_database(insert=False, skip_projects_in_kickstarter=False)
        pass
        # download_all_of_kickstarter_into_database(start_index=44110)
        # useful_functions.send_scott_a_text('test message')
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        try:
            useful_functions.send_scott_a_text(message='Kickstarter update failed')
        except Exception:
            logging.error('Message sending failed too!')
            logging.error(traceback.format_exc())
        logging.debug('Current path:\n{}'.format(sys.path))
        raise
    logging.info('Update was successful.')
