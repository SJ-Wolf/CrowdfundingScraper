import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
import requests
import os
import time
import logging
import db_connections
import traceback
from lxml import etree
from io import StringIO, BytesIO
# from slimit import ast
# from slimit.parser import Parser
# from slimit.visitors import nodevisitor
import subprocess
import json
from lxml.html.soupparser import fromstring
import re
import xml.etree.ElementTree
# import pandas as pd
# import dateutil.parser
# import pytz
from useful_functions import remove_chars_from_string, split_array_into_chunks, split_list, get_file_names_from_url_file
# import sitemap_downloader
import timestring
import datetime

# import MySQLdb
try:
    import cPickle as pickle
except:
    import pickle

'''
DEFINING_CSS_IDS = dict(
    fundraiser='total_raised',  # https://www.crowdrise.com/straightouttabergenf/fundraiser/harleyungar
    user='myFundraisersContainer',  # https://www.crowdrise.com/debradahlyoung
    charity='fundraisers_section_title',  # https://www.crowdrise.com/woundedwarriorproject
                                          # https://www.crowdrise.com/NYRRYouth
    event='event_page',  # https://www.crowdrise.com/ToughMudderTri-State
    special_user='custom_share'
)
'''
REQUEST_HEADERS = {'User-agent': 'Mozilla/5.0'}


# extremely slow, only to be used as last resort
def get_js_fields(js_code, variable_name='args'):
    js_tree = Parser().parse(js_code)

    fields = dict()
    for node in nodevisitor.visit(js_tree):
        if not isinstance(node, ast.VarDecl):
            continue
        if getattr(node.identifier, 'value', '') != variable_name:
            continue
        for prop in getattr(node.initializer, 'properties', []):
            left_side_of_assignment = getattr(prop.left, 'value', '').strip("'//").strip('"')
            right_side_of_assignment = getattr(prop.right, 'value', '').strip("'//").strip('"')
            if right_side_of_assignment == 'N':
                right_side_of_assignment = False
            elif right_side_of_assignment in ('none', ''):
                right_side_of_assignment = None
            if left_side_of_assignment in fields.keys() and fields[left_side_of_assignment] != right_side_of_assignment:
                raise Exception('Existing value of {} for key {} does not equal new value of {}'.format(
                    fields[left_side_of_assignment], left_side_of_assignment, right_side_of_assignment))
            fields[left_side_of_assignment] = right_side_of_assignment
    return fields


class CrowdriseScraper():
    DEFINING_XPATH = dict(
        fundraiser='//div[contains(@class, "fundTitle")]',
        # https://www.crowdrise.com/straightouttabergenf/fundraiser/harleyungar
        user='//*[@id="myFundraisersContainer"]',  # https://www.crowdrise.com/debradahlyoung
        charity='//*[@id="fundraisers_section_title"]',  # https://www.crowdrise.com/woundedwarriorproject
        # https://www.crowdrise.com/NYRRYouth
        event='//*[@id="event_page"]',  # https://www.crowdrise.com/ToughMudderTri-State
        special_user='//*[@id="custom_share"]',  # spartan
        front_page_redirect='//div[@class="jumbotron large"]'

    )

    def __init__(self, process_num=None):
        self.parser = etree.HTMLParser()
        self.process_num = process_num
        if os.path.isfile('static_pages.txt'):
            with open('static_pages.txt') as f:
                self.static_pages = set(f.read().split('\n'))
        else:
            self.static_pages = None

        with open('pages_to_redownload.txt') as f:
            self.pages_to_redownload = set(
                [x.replace('\n', '').replace('\r', '').replace('https://', '') for x in f.readlines()])

        self.crowdrise_data = dict()
        self.clear_crowdrise_data()

    def get_tree_from_file(self, cur_file_name):
        with open(cur_file_name, 'rb') as f:
            return self.get_tree_from_text(f.read())

    def get_tree_from_text(self, text):
        f_raw = text
        f_re = re.sub("<!--.*?--!>", "", f_raw)  # kill these bad comments
        f_re = re.sub("<!--.*?-->", "", f_re)  # kill good comments
        f_removed_whitespace = f_re.translate(None, '\n\t\r')
        f_unicode = unicode(f_removed_whitespace, encoding='utf-8', errors='replace')
        try:
            tree = fromstring(f_removed_whitespace)
        except RuntimeError:
            logging.warning('RuntimeError on {} when trying to parse (stack overflow?)'.format(cur_file_name))
            f_string = StringIO(f_unicode)
            tree = etree.parse(f_string, self.parser)
        except ValueError:
            logging.warning(
                'RuntimeError on {} when trying to parse (incompatible character?)'.format(cur_file_name))
            f_string = StringIO(f_unicode)
            tree = etree.parse(f_string, self.parser)
        return tree

    def scrape_tree(self, tree, url, file_index, latest_comment_id=None):
        """
        if '/sitemap/' in cur_file_name:
            return
        if '.' in CrowdriseScraper.get_last_section_of_url(cur_file_name):
            url = cur_file_name[:cur_file_name.rfind('.')]
        else:
            url = cur_file_name
        """
        if url in self.static_pages:
            logging.info('{}\tskipping {} since it is a static page'.format(file_index, url))
            return

        if url in self.pages_to_redownload:
            logging.info('{}\tskipping {} since it is in pages_to_redownload'.format(file_index, url))
            return
        try:
            # useful for checking if there has been a redirect
            true_url = tree.xpath(
                '//meta[@property="og:url"]')[0].attrib['content'].replace('https://', '').replace('http://', '')
            if true_url[-1] == '/':
                true_url = true_url[:-1]
            if '/fundraiser/' in true_url:
                base_true_url = true_url[:true_url.rfind('/fundraisers/')]
            else:
                base_true_url = true_url

            if 'www.crowdrise.com/signin/form/' in true_url:
                logging.info('{}\tskipping {} since it redirected to a login page'.format(file_index, url))
                return

            if true_url in self.static_pages:
                logging.info('{}\tskipping {} since it redirected to a static page'.format(file_index, url))
                return

            page_type = CrowdriseScraper.get_page_type(tree)
            logging.debug('{}\tfile {} is a {}'.format(file_index, url, page_type))
            if page_type == 'fundraiser':
                if os.name == 'nt':  # on windows
                    if url.lower().replace('e:\\', '').replace('\\', '/') != base_true_url.lower().replace('https://',
                                                                                                           ''):
                        logging.debug('skipping {} since url!=base_true_url')
                        return
                else:
                    if url.lower() != base_true_url.replace('https://', '').lower():
                        logging.debug('skipping {} since url!=base_true_url')
                        return
            file_data = CrowdriseScraper.get_crowdrise_data(page_type, tree, url, latest_comment_id=latest_comment_id)
            # file_data['file_path'] = cur_file_name
            file_data['url'] = url
            file_data['true_url'] = true_url
            file_data['base_true_url'] = base_true_url

            # file_data['last_scrape'] = time.gmtime(os.path.getmtime(cur_file_name))

            # handle data that requires its own table - eg the fundraisers each user has
            if 'projects' in file_data.keys():
                projects = file_data.pop('projects')
                self.crowdrise_data['user_project'] += [{'username': file_data['username'],
                                                         'project': 'www.crowdrise.com' + x} for x in projects]
            if 'events' in file_data.keys():
                events = file_data.pop('events')
                self.crowdrise_data['charity_event'] += [{'charity': file_data['url'],
                                                          'event': 'www.crowdrise.com' + x} for x in events]
            if 'team_members' in file_data.keys():
                team_members = file_data.pop('team_members')
                self.crowdrise_data['team'] += team_members

            if 'donations' in file_data.keys():
                donations = file_data.pop('donations')
                self.crowdrise_data['donation'] += donations

            self.crowdrise_data[page_type].append(file_data)
        except Exception:
            logging.error('{}\tfailed on {}'.format(file_index, url))
            logging.error(traceback.format_exc())
            raise

    def scrape_file(self, cur_file_name, file_index, latest_comment_id=None):
        self.scrape_tree(tree=self.get_tree_from_file(cur_file_name),
                         url=cur_file_name,
                         file_index=file_index,
                         latest_comment_id=latest_comment_id)

    def upload_crowdrise_data(self, db):
        db_connections.multi_table_upload(data=self.crowdrise_data,
                                          db=db,
                                          ensure=True,
                                          process_num=self.process_num)
        self.clear_crowdrise_data()

    def clear_crowdrise_data(self):
        self.crowdrise_data = dict(
            fundraiser=[],
            user=[],
            charity=[],
            event=[],
            special_user=[],
            user_project=[],
            charity_event=[],
            front_page_redirect=[],
            team=[],
            donation=[],
        )

    # no longer necessary as any urls with a folder are skipped
    '''
    @staticmethod
    def generate_static_pages(db):
        if not os.path.isfile('static_pages.txt'):
            with open('static_pages.txt', 'w') as f:
                for row in db.query(
                        "select REPLACE(loc, 'https://', '') as url from sitemap where category = 'static'"):
                    f.write('{}\n'.format(row['url']))
    '''

    @staticmethod
    def get_page_type(tree):
        page_type = None
        '''
        for key, item in DEFINING_CSS_IDS.iteritems():
            if len(tree.xpath('//*[@id="{}"]'.format(item))) > 0:
                if page_type is not None:
                    raise Exception('Matches multiple page types!')
                else:
                    page_type = key
        '''
        for key, item in CrowdriseScraper.DEFINING_XPATH.items():
            if len(tree.xpath(item)) > 0:
                if page_type is not None:
                    raise Exception('Matches multiple page types!')
                else:
                    page_type = key
        if page_type is None:
            raise NotImplementedError('Matches no page types!')
        return page_type

    @staticmethod
    def get_crowdrise_data(page_type, tree, url, latest_comment_id=None):
        if page_type == 'fundraiser':
            # return {}
            return CrowdriseScraper.get_fundraiser_data(tree, url, latest_comment_id=latest_comment_id)
        elif page_type == 'user':
            # return {}
            return CrowdriseScraper.get_user_data(tree)
        elif page_type == 'charity':
            # return {}
            return CrowdriseScraper.get_charity_data(tree)
        elif page_type == 'event':
            # return {}
            return CrowdriseScraper.get_event_data(tree)
        elif page_type == 'special_user':
            # return {}
            return CrowdriseScraper.get_special_user_data(tree)
        elif page_type == 'front_page_redirect':
            return {}
        else:
            raise Exception('Unknown page type: {}'.format(page_type))

    @staticmethod
    def safe_get_item_in_xpath(tree, path, index=0):
        try:
            return tree.xpath(path)[index]
        except IndexError:
            return None

    @staticmethod
    def get_last_section_of_url(url):
        return url[url.rfind('/') + 1:]

    @staticmethod
    def sanitize_money_entry(money_text):
        money_text = money_text.strip().replace(',', '')
        if money_text[0] != '$' and money_text != '0':
            raise Exception('Unexpected money text (must start with $): {}'.format(money_text))
        if money_text[0] == '$':
            money_text = money_text[1:]
        return float(money_text)

    @staticmethod
    def sanitize_number_entry(number_text):
        number_text = number_text.strip().replace(',', '')
        return float(number_text)

    @staticmethod
    def get_fundraiser_data(tree, url, latest_comment_id=None):
        data = dict()

        total_raised = tree.xpath('//*[@id="total_raised"]/h3/text()')
        if len(total_raised) > 0:
            data['total_raised'] = CrowdriseScraper.sanitize_money_entry(total_raised[0])
        else:
            data['total_raised'] = None

        team_total_raised = tree.xpath('//*[@id="the_team_title"]/text()')
        if len(team_total_raised) > 0:
            assert 'The Team:' in team_total_raised[0]
            if team_total_raised[0].replace('The Team:', '').strip() == '':
                data['team_total_raised'] = None
            else:
                data['team_total_raised'] = CrowdriseScraper.sanitize_money_entry(
                    team_total_raised[0].replace('The Team:', ''))
        else:
            data['team_total_raised'] = None

        if data['team_total_raised'] is not None and data['total_raised'] != data['team_total_raised']:
            return None

        benefiting_line = tree.xpath('//p[@id="benefiting_line"]/a')
        if len(benefiting_line) > 0:
            data['benefiting'] = 'www.crowdrise.com' + benefiting_line[0].attrib['href']
        else:
            data['benefiting'] = None

        event_line = tree.xpath('//p[@id="event_line"]/a')
        if len(event_line) > 0:
            data['event'] = 'www.crowdrise.com' + event_line[0].attrib['href']
        else:
            data['event'] = None

        event_date_line = tree.xpath('//p[@id="event_date_line"]/span/text()')
        if len(event_date_line) > 0:
            event_date_text = event_date_line[0].strip()
            data['event_date'] = CrowdriseScraper.interpret_date(event_date_text)
        else:
            data['event_date'] = None

        organizer_line = tree.xpath('//*[@id="organizer_line"]/a')
        if len(organizer_line) > 0:
            organizer_fundraiser_url = organizer_line[0].attrib['href']
            data['parent_organizer_fundraiser_url'] = organizer_fundraiser_url
            data['parent_organizer_username'] = CrowdriseScraper.get_last_section_of_url(organizer_fundraiser_url)
        else:
            data['parent_organizer_fundraiser_url'] = None
            data['parent_organizer_username'] = None

        story = tree.xpath('//div[@id="content"]')
        if len(story) > 0:
            # data['story'] = b'\n'.join(etree.tostring(story[0], pretty_print=True, method='html').split(b'\n')[1:])
            data['story'] = etree.tostring(story[0], method='text', encoding='utf8')
        else:
            data['story'] = None

        fundraiser_type_section = tree.xpath('//div[@class="fundraiser-type"]/p/span/text()')
        if len(fundraiser_type_section) > 0:
            fundraiser_type = fundraiser_type_section[0]
        else:
            fundraiser_type = None

        if fundraiser_type == 'direct to organizer':  # direct to organizer fundraiser
            data['direct_to_organizer'] = True
            data['username'] = tree.xpath(
                '//div[contains(@class, "ffa-title")]/h4/a')[0].attrib['data-profile'].split('|')[1]
        elif len(fundraiser_type_section) == 2 and 'direct to organizer' in fundraiser_type_section[1]:
            fundraiser_type = fundraiser_type_section[1]
            logging.warning('fundraiser {} has more than than one fundraiser_type section. That section = {}'.format(
                url, fundraiser_type
            ))
            data['direct_to_organizer'] = True
            data['username'] = tree.xpath(
                '//div[contains(@class, "ffa-title")]/h4/a')[0].attrib['data-profile'].split('|')[1]
        elif fundraiser_type is None:
            data['direct_to_organizer'] = False
            try:
                data['username'] = CrowdriseScraper.get_last_section_of_url(
                    tree.xpath('//a[contains(@class, "sponsorProject button")]')[0].attrib['href'])
            except IndexError:
                try:
                    data['username'] = CrowdriseScraper.get_last_section_of_url(
                        tree.xpath('//a[contains(@class, "donateRegistry button")]')[0].attrib['href'])
                except IndexError:
                    try:
                        profile_pic_alt_text = tree.xpath('//a[@class="fundraiser_profile_link"]/img')[0].attrib['alt']
                        data['username'] = profile_pic_alt_text[:profile_pic_alt_text.index("'s")]
                    except KeyError:
                        data['username'] = None
        else:
            raise Exception('Unknown fundraiser_type: {}'.format(fundraiser_type))

        # get team members, if any
        team_members = []

        def scrape_team_element(element):
            team_member = dict(username=None,
                               fundraiser_url=None,
                               amount_raised=None,
                               fundraiser_id=None,
                               project_id=None,
                               charity_id=None,
                               goal=None)

            team_member['username'] = CrowdriseScraper.get_last_section_of_url(
                element.xpath('./div/div/h4/a')[0].attrib['href'])
            team_member['fundraiser_url'] = url
            try:
                team_member['amount_raised'] = CrowdriseScraper.sanitize_money_entry(
                    element.xpath('./div//h3/text()')[0])
            except IndexError:
                team_member['amount_raised'] = None

            try:
                progress_text = element.xpath('.//p[@class="progressText"]/span/text()')[0]
            except IndexError:
                progress_text = None
            if progress_text is not None:
                team_member['goal'] = CrowdriseScraper.sanitize_money_entry(
                    progress_text.split('Raised of')[1].replace('Goal', '').strip())

            for attrib in element.attrib['class'].split(' '):
                attrib_split = attrib.split('_')
                if attrib_split[0] == 'project':
                    if attrib_split[1] == '':
                        return None
                    else:
                        team_member['project_id'] = int(attrib_split[1])
                elif attrib_split[0] == 'fundraiser':
                    team_member['fundraiser_id'] = int(attrib_split[1])
                elif attrib_split[0] == 'charity':
                    team_member['charity_id'] = int(attrib_split[1])
            return team_member

        if len(tree.xpath('//*[@id="the_team_title"]')) > 0:
            if len(tree.xpath('//*[@id="seeMoreDonations"]')) > 0:  # > 8 donations:
                more_team_members_url = 'https://www.crowdrise.com' + \
                                        tree.xpath('//*[@id="seeMoreDonations"]')[0].attrib['href']
                cur_url = more_team_members_url
                page = 1
                while True:
                    comment_request = requests.get(cur_url, headers=REQUEST_HEADERS)
                    more_team_members_tree = fromstring(comment_request.text)
                    assert str(comment_request.status_code)[0] == '2'
                    for team_element in more_team_members_tree.xpath(
                            '//div[@class="teamMemberContainer"]/div[contains(@class, "grid1-4")]'):
                        cur_team_member = scrape_team_element(team_element)
                        if cur_team_member is not None:
                            good_username = cur_team_member['username']
                            team_members.append(cur_team_member)
                    if len(more_team_members_tree.xpath('//*[@id="seeMoreTeams"]')) > 0:
                        page += 1
                        if len(more_team_members_url.split('/')) == 6:
                            cur_url = "{}/{}/{}".format(more_team_members_url,
                                                        CrowdriseScraper.get_last_section_of_url(good_username),
                                                        page)
                        else:
                            cur_url = "{}/{}".format(more_team_members_url, page)
                    else:
                        break
            else:  # <= 8 donations
                for team_element in tree.xpath('//*[@id="the_team_title"]/../div[contains(@class, "grid1-4")]'):
                    cur_team_member = scrape_team_element(team_element)
                    if cur_team_member is not None:
                        team_members.append(cur_team_member)
        data['team_members'] = team_members

        donations = CrowdriseScraper.get_comments(tree=tree, url=url, latest_comment_id=latest_comment_id)
        data['num_comments'] = len(donations)
        data['donations'] = donations

        d = dict()
        for i, elem in enumerate(tree.xpath('//script')):
            if elem.text is not None:
                js_code = elem.text
                for line in js_code.replace('{', ',').replace('}', ',').replace(';', ',').replace('\n', ',').split(","):
                    split_line = line.split(':')
                    if len(split_line) == 2:
                        left_side = split_line[0].strip("""" ',\\\t""").replace("\\'", "'").replace('\\"', '"')
                        right_side = split_line[1].strip("""" ',\\\t""").replace("\\'", "'").replace('\\"', '"')
                        if right_side == 'N':
                            right_side = False
                        elif right_side == 'Y':
                            right_side = True
                        elif right_side in ('none', ''):
                            right_side = None
                        d[left_side] = right_side

        for key in (
                'about', 'campaign_id', 'fundraiser_id', 'is_organizer', 'member_type', 'project_id', 'project_name',
                'project_username', 'owner_username', 'username'):
            if key in d.keys():
                data[key] = d[key]
            elif key not in data.keys():
                data[key] = None
        return data

    @staticmethod
    def get_special_user_data(tree):
        data = dict()

        return data

    @staticmethod
    def make_user_fundraiser_request(username, page_num):
        parser = etree.HTMLParser()
        p = subprocess.check_output(
            """curl "https://www.crowdrise.com/profiles/member_fundraisers/{}/{}" -H "Cookie: LPVID=paTPgrKUQUSPx723AG9fsw; LPSID-67125380=6lD2Mg9DSOCyqQS7yd9UwA.0ef15a19e2c47ed0e7ff857d83e3cb7ee88530ef; LPCKEY-67125380=b47fd344-f7ce-4b59-a004-47d17d24e1eca-81423"%"7Cnull"%"7Cnull"%"7C40; crowdrise_session=c75654cdd34e91976b6603cc5fdbc596" -H "X-NewRelic-ID: UwEDUFFbGwYGV1NbBgg=" -H "DNT: 1" -H "Accept-Encoding: gzip, deflate, sdch, br" -H "Accept-Language: en-US,en;q=0.8" -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36" -H "Accept: application/json, text/javascript, */*; q=0.01" -H "cache-control: no-cache" -H "X-Requested-With: XMLHttpRequest" -H "Connection: keep-alive" -H "Referer: https://www.crowdrise.com/jordanmetzl" --compressed"""
                .format(username, page_num), shell=True)
        new_project_html = json.loads(p)['projects']
        return etree.parse(StringIO(new_project_html), parser)

    @staticmethod
    def get_user_data(tree):
        user_data = dict()
        url = tree.xpath('/html/head/link[@rel="canonical"]')[0].attrib['href']
        user_data['impact_points'] = CrowdriseScraper.sanitize_number_entry(tree.xpath('//*[@id="score"]/text()')[0])
        try:
            user_data['money_raised'] = CrowdriseScraper.sanitize_money_entry(
                tree.xpath('//div[contains(@class, "moneyRaised")]/a/h3/text()')[0])
        except IndexError:
            user_data['money_raised'] = None
        try:
            user_data['donate_link'] = 'www.crowdrise.com' + \
                                       tree.xpath('//a[contains(@class, "sponsorMember")]')[0].attrib['href']
        except IndexError:
            user_data['donate_link'] = None
        projects = [x.attrib['href'] for x in tree.xpath('//div[@class="grid1-4 project"]//h4/a')]
        user_data['username'] = url.split('/')[-1]
        if len(tree.xpath('//*[@id="showMoreFundraisers"]')) > 0:  # more projects than shown
            page_num = 2
            while True:
                additional_fundraiser_tree = CrowdriseScraper.make_user_fundraiser_request(
                    username=user_data['username'],
                    page_num=page_num)
                try:
                    additional_projects = [x.attrib['href'] for x in
                                           additional_fundraiser_tree.xpath('//div[@class="grid1-4 project"]//h4/a')]
                except AssertionError:  # invalid html => no more projects
                    break
                projects += additional_projects
                page_num += 1
                if len(additional_projects) < 8:
                    break
        try:
            user_data['full_name'] = tree.xpath('//span[@class="memberTopNameBig"]/text()')[0]
        except:
            user_data['full_name'] = None

        # getting info from tooltip
        user_data['join_date'] = None
        user_data['about'] = None
        user_data['location'] = None
        stuff_about_me_flag = False
        for index, row in enumerate([x.strip() for x in tree.xpath('//*[@id="memberAboutTooltip"]//text()')
                                     if x.strip() != '']):
            if row[:len('CROWDRISING SINCE: ')] == 'CROWDRISING SINCE: ':
                user_data['join_date'] = CrowdriseScraper.interpret_date(row.replace('CROWDRISING SINCE: ', ''))
            elif index == 0:  # location line
                user_data['location'] = ' '.join(row.split()).replace(' ,', ',')
            if stuff_about_me_flag:
                user_data['about'] = row
                stuff_about_me_flag = False
            if row == 'Stuff About Me:':
                stuff_about_me_flag = True

        user_data['projects'] = projects
        user_data['number_of_projects'] = len(projects)
        return user_data

    @staticmethod
    def get_charity_data(tree):
        data = dict()
        data['has_video'] = len(tree.xpath('//*[@id="player"]')) > 0
        try:
            data['money_raised'] = CrowdriseScraper.sanitize_money_entry(
                tree.xpath('//div[contains(@class, "moneyRaised")]/h3/text()')[0])
        except IndexError:
            data['money_raised'] = None

        try:
            data['name'] = tree.xpath('//*[@id="charity_name"]/text()')[0].strip()
        except IndexError:
            data['name'] = None

        data['mission_html'] = etree.tostring(tree.xpath('//*[@id="charityMissionScroll"]/..')[0], pretty_print=True)
        '''
        data['mission_subtitle'] = tree.xpath('//*[@id="charityMissionScrollInner"]//*[@class="subtitle"]/text()')[0].strip()
        mission_texts = tree.xpath('//*[@id="content"]/p/span/text()')
        if len(mission_texts) == 0:
            data['mission_text'] = tree.xpath('//*[@id="content"]/text()')[0].strip()
        else:
            data['mission_text'] = '\n'.join([x.strip() for x in mission_texts])
        '''
        events = [x.attrib['href'] for x in tree.xpath('//*[@id="memberEventsGrid"]//div[@class="content"]/a')]

        data['location'] = None
        for row in [remove_chars_from_string(q, '\n\t').strip() for q in
                    tree.xpath('//*[@id="memberAboutTooltip"]/div/text()')]:
            if row[:6] == 'BASED:':
                data['location'] = row[6:].strip()
                break

        start_date_text = tree.xpath('//*[@id="memberAboutTooltip"]//span/text()')[0].strip()
        data['started'] = CrowdriseScraper.interpret_date(start_date_text)

        site_and_ein_section = tree.xpath('//*[@id="main-content"]//div[@class="siteAndEIN"]')
        data['ein'] = None
        data['site'] = None
        if len(site_and_ein_section) > 0:
            try:
                data['ein'] = ''.join(
                    x for x in site_and_ein_section[0].xpath('./text()')[0].strip()
                    if x in ([str(q) for q in range(10)] + ['-']))
            except IndexError:
                pass
            try:
                data['site'] = site_and_ein_section[0].xpath('./a')[0].attrib['href']
            except IndexError:
                pass
        data['num_events'] = len(events)
        data['events'] = events

        return data

    @staticmethod
    def get_event_data(tree):
        data = dict()
        data['amount_raised'] = CrowdriseScraper.sanitize_money_entry(
            tree.xpath('//*[@id="total_raised_amount"]/text()')[0])
        try:
            data['goal'] = CrowdriseScraper.sanitize_money_entry(' '.join(
                tree.xpath('//*[@id="campaignMoneyRaised"]//p[@class="progressText"]/span/text()')[0]
                    .split()).split(' Raised of ')[1].split(' Goal')[0].strip())
        except IndexError:
            data['goal'] = None

        data['event_date'] = None
        data['deadline_to_give'] = None
        for event_date_element in tree.xpath('//*[@id="event_dates"]//span'):
            if event_date_element.xpath('../text()')[0].upper().find('EVENT DATE') != -1:
                data['event_date'] = CrowdriseScraper.interpret_date(event_date_element.text)
            elif event_date_element.xpath('../text()')[0].upper().find('DEADLINE TO GIVE') != -1:
                data['deadline_to_give'] = CrowdriseScraper.interpret_date(event_date_element.text)
        data['story'] = '\n'.join(tree.xpath('//*[@id="content"]//p/text()'))
        data['has_video'] = len(tree.xpath('//*[@id="video"]')) > 0
        return data

    @staticmethod
    def interpret_date(date_string):
        date_string = date_string.strip()
        try:
            date_output = time.strptime(date_string, '%b %d, %Y')
        except ValueError:  # probably has a specific time at the end
            from dateutil import parser as date_parser
            date_output = date_parser.parse(date_string)
        return date_output

    # TODO: Make sure to grab the comments at some point...
    @staticmethod
    def get_comments(tree, url, latest_comment_id=None):
        return []

        def get_comments_from_tree(comment_tree):
            comments = []
            for comment in comment_tree.xpath(
                    '//div[@class="container"]/div[@class="full"]//div[@class="title fLeft"]'):
                row = dict()
                row['url'] = url
                try:
                    row['name'] = comment.xpath('.//h4/a/text()')[0].strip()
                    row['username'] = comment.xpath('.//h4/a')[0].attrib['href']
                except IndexError:
                    row['username'] = None
                    try:
                        row['name'] = comment.xpath('.//h4/text()')[0].strip()
                    except IndexError:
                        row['name'] = None

                h5_texts = [x.strip() for x in comment.xpath('.//h5/text()')]
                row['amount'] = None
                row['charity'] = None
                for text in h5_texts:
                    if 'DONATION: ' in text:
                        row['amount'] = CrowdriseScraper.sanitize_money_entry(text.replace('DONATION: ', ''))
                    elif 'CHARITY: ' in text:
                        row['charity'] = text.replace('CHARITY: ', '')

                try:
                    row['message'] = comment.xpath('.//p/span/text()')[0].strip()
                except IndexError:
                    row['message'] = None
                try:
                    row['date'] = timestring.Date(comment.xpath('.//i/text()')[0].strip()).date
                except IndexError:
                    logging.warning('{}: missing date field. Skipping comment.'.format(url))
                    continue
                try:
                    row['id'] = int(comment.xpath('..')[0].attrib['id'].replace('comment_', ''))
                except IndexError:
                    logging.warning('{}: missing id field. Skipping comment.'.format(url))
                    continue
                comments.append(row)
            return comments

        if latest_comment_id is not None and type(latest_comment_id) != int:
            latest_comment_id = int(latest_comment_id)
        try:
            additional_comments_url = tree.xpath('//*[@id="donorComments"]//a[@class="more"]')[0].attrib['href']
        except IndexError:
            additional_comments_url = None

        if additional_comments_url is None:  # scrape comments in current tree
            donations = get_comments_from_tree(tree)
        else:  # scrape comments in new url(s)

            # see if we can get away with using the comments in the html
            if latest_comment_id is not None:
                cur_comments = get_comments_from_tree(tree)
                if latest_comment_id in [int(x['id']) for x in cur_comments]:
                    return cur_comments
            donations = []
            cur_comment_page = 0
            cur_additional_comments_url = additional_comments_url
            while True:
                comment_request = requests.get('https://www.crowdrise.com' + cur_additional_comments_url, headers=REQUEST_HEADERS)
                assert str(comment_request.status_code)[0] == '2'
                comment_tree = fromstring(comment_request.text)
                cur_comments = get_comments_from_tree(comment_tree)
                if len(cur_comments) == 0:
                    break
                if latest_comment_id is not None:
                    comment_ids = [int(x['id']) for x in cur_comments]
                    if latest_comment_id > max(comment_ids) and cur_comment_page == 0:
                        # raise Exception('latest_comment_id ({}) > max(comment_ids) ({}) on first comment page'.format(
                        #    latest_comment_id, max(comment_ids)
                        # ))
                        logging.warning('latest_comment_id ({}) > max(comment_ids) ({}) on first comment page'.format(
                            latest_comment_id, max(comment_ids)))
                        return donations + cur_comments
                    if latest_comment_id in comment_ids:
                        return donations + cur_comments
                donations += cur_comments
                if len(comment_tree.xpath('//*[@id="seeMoreDonations"]')) == 1:
                    cur_comment_page += 1
                    cur_additional_comments_url = additional_comments_url + '/' + str(cur_comment_page)
                else:
                    break
        return donations


def insert_all_files_into_db(infile=None, db=db_connections.get_fungrosencrantz_schema('crowdrise')):
    file_data = []
    if infile is None:
        for root, directories, filenames in os.walk('E:\\www.crowdrise.com'):
            for filename in filenames:
                cur_file_name = os.path.join(root, filename)
                file_data.append(dict(file_name=cur_file_name))
    else:
        file_data = [dict(file_name=x) for x in get_file_names_from_url_file(infile=infile)]
    # db['all_files'].insert_many(rows=file_data, ensure=False)
    db_connections.uploadOutputFile(data=file_data, db=db, table='all_files')


def scrape_all_files(process_num=None, total_processes=None, infile='urls_to_scrape',
                     db=db_connections.get_fungrosencrantz_schema('crowdrise')):
    # wait for processes to connect to db before continuing
    # otherwise they may encounter a temporary table created by another process and crash when trying to get metadata
    print('process {}: waiting start'.format(process_num))
    if total_processes is not None:
        time.sleep(3)
    print('process {}: waiting done'.format(process_num))

    scraper = CrowdriseScraper(process_num=process_num)

    file_index = 0
    last_file_index = float('inf')
    # last_file_index = 505
    chunk_size = 500 * total_processes
    q = """
select file_name from all_files left join html
on all_files.file_name = html.url
where html.url is null
order by file_name
"""
    q = """
select url as file_name from fundraiser order by RAND(10);
    """
    # files_to_scrape = [x['file_name'] for x in db.query(q)]
    files_to_scrape = []
    with open(infile) as f:
        for i, line in enumerate(f.readlines()):
            if process_num is not None and i % total_processes != process_num:
                continue
            file_path = line[:-1].replace('https://', '')
            if os.name == 'nt':
                file_path = os.path.join('E:\\', file_path.replace('/', '\\'))
            files_to_scrape.append(file_path)

    db['urls_to_scrape_{}'.format(process_num)].insert_many([dict(url=x) for x in files_to_scrape], ensure=True)
    url_data = [x for x in db.query('''
        select {0}.url, t1.latest_comment_id
        from {0} left JOIN
            (SELECT
                MAX(id) as latest_comment_id, url
            FROM
                donation
            GROUP BY url) AS t1 ON {0}.url = t1.url;
    '''.format('urls_to_scrape_{}'.format(process_num)))]
    db['urls_to_scrape_{}'.format(process_num)].drop()
    print('process {}: done getting files'.format(process_num))
    time.sleep(10)  # make sure all processes have gotten this list of files
    print('process {}: done waiting after getting files'.format(process_num))
    for chunk in split_array_into_chunks(url_data, chunk_size=chunk_size):
        html_data = []
        for row in chunk:
            cur_file_name = row['url']
            latest_comment_id = row['latest_comment_id']

            # cur_file_name = os.path.join(root, filename)
            try:
                scraper.scrape_file(cur_file_name, file_index, latest_comment_id=latest_comment_id)
            except:
                logging.error(traceback.format_exc())
            else:
                html_data.append(dict(
                    url=cur_file_name,
                    last_scrape=time.gmtime(os.path.getmtime(cur_file_name)),
                ))
            file_index += 1
            if file_index >= last_file_index:
                break
                # if file_index % chunk_size == 0 and file_index != 0:
        scraper.upload_crowdrise_data(db)
        db_connections.uploadOutputFile(data=html_data, db=db, table='html', process_num=process_num)
        if file_index >= last_file_index:
            break

            # for table, data in crowdrise_data.items():
            #    db[table].drop()
            #    db[table].insert_many(data)
            # db['user'].drop()
            # db['user'].insert_many(crowdrise_data['user'])
            # db['charity'].drop()
            # db['charity'].insert_many(crowdrise_data['charity'])
            # db_connections.uploadOutputFile(data=crowdrise_data['fundraiser'], db=db, table='fundraiser')

            # craper.upload_crowdrise_data(db)
            # db_connections.uploadOutputFile(data=html_data, db=db, table='html', process_num=process_num)


def analyze_sitemap_xml(url):
    r = requests.get(url, headers=REQUEST_HEADERS)
    assert str(comment_request.status_code)[0] == '2'
    root = xml.etree.ElementTree.fromstring(r.content)
    root_tag = root.tag
    namespace = root_tag[root_tag.index('{'):root_tag.index('}') + 1]
    url_data = []
    for url in root.findall('.//{}url'.format(namespace)):
        loc = url.find('./{}loc'.format(namespace))
        if loc is not None:
            loc = loc.text.strip()
        lastmod = url.find('./{}lastmod'.format(namespace))
        if lastmod is not None:
            lastmod = dateutil.parser.parse(lastmod.text.strip())
        changefreq = url.find('./{}changefreq'.format(namespace))
        if changefreq is not None:
            changefreq = changefreq.text.strip()
        priority = url.find('./{}priority'.format(namespace))
        if priority is not None:
            priority = priority.text.strip()

        url_data.append(
            dict(
                loc=loc,
                lastmod=lastmod,
                changefreq=changefreq,
                priority=priority,
            )
        )
    return url_data


def get_sitemap_urls():
    r = requests.get("https://www.crowdrise.com/sitemap/xml-index", headers=REQUEST_HEADERS)
    assert str(comment_request.status_code)[0] == '2'
    root = xml.etree.ElementTree.fromstring(r.content)

    return [x.text for x in root.findall('.//') if "www.crowdrise.com/sitemap/xml" in x.text]


def update_fundraiser_donations(num_workers=4):
    db = db_connections.get_fungrosencrantz_schema('crowdrise')
    if len(sys.argv) == 1:  # no arguments -> first run -> split up work
        fundraisers_no_comments = [x for x in db.query(
            'select url, url as file_path from fundraiser where fundraiser.num_comments is null order by rand()')]
        fundraisers_no_comments_list = split_list(fundraisers_no_comments, wanted_parts=num_workers)
        for i, chunk in enumerate(fundraisers_no_comments_list):
            with open('fundraisers_no_comments_{0}'.format(i), 'w') as f:
                json.dump(chunk, f)
        processes = []
        for index in range(num_workers):
            processes.append(
                subprocess.Popen('python crowdrise_scraper.py {0} fundraisers_no_comments_{0}'.format(index),
                                 shell=True))
        try:
            for i, p in enumerate(processes):
                logging.debug('waiting for process {}'.format(i))
                p.wait()
        except:
            for p in processes:
                # Get the process id & try to terminate it gracefuly
                p.terminate()

                # Check if the process has really terminated & force kill if not.
                try:
                    os.kill(p.pid, 0)
                    p.kill()
                    print("Forced kill")
                except OSError:
                    print("Terminated gracefully")

            raise
    else:
        process_index = int(sys.argv[1])
        fundraisers_no_comments_file_name = sys.argv[2]

        with open(fundraisers_no_comments_file_name) as f:
            fundraisers_no_comments = json.load(f)

        parser = etree.HTMLParser()
        for chunk in split_array_into_chunks(
                fundraisers_no_comments,
                chunk_size=500):
            comments = []
            num_comments_data = []

            for row in chunk:
                file_path = row['url']
                try:
                    f = open(file_path)
                except IOError:
                    f = open(file_path + '.1')
                f_raw = f.read()
                f_re = re.sub("<!--.*?--!>", "", f_raw)  # kill these bad comments
                f_re = re.sub("<!--.*?-->", "", f_re)  # kill good comments
                f_removed_whitespace = f_re.translate(None, '\n\t\r')
                f_unicode = unicode(f_removed_whitespace, encoding='utf-8', errors='replace')
                try:
                    tree = fromstring(f_removed_whitespace)
                except RuntimeError:
                    logging.warning('RuntimeError on {} when trying to parse (stack overflow?)'.format(file_path))
                    f_string = StringIO(f_unicode)
                    tree = etree.parse(f_string, parser)
                except ValueError:
                    logging.warning(
                        'RuntimeError on {} when trying to parse (incompatible character?)'.format(file_path))
                    f_string = StringIO(f_unicode)
                    tree = etree.parse(f_string, parser)
                try:
                    cur_comments = CrowdriseScraper.get_comments(tree, file_path)
                except:
                    logging.error('Failure. file_path={}'.format(file_path))
                    logging.error(traceback.format_exc())
                    continue
                num_comments_data.append(dict(num_comments=len(cur_comments), url=file_path))
                comments += cur_comments
            # make this an insert as this could fail with multiple processes
            # db_connections.uploadOutputFile(data=comments, db=db, table='donation')
            try:
                db.begin()
                db['donation'].insert_many(comments, ensure=False)
                for row in num_comments_data:
                    db.query("UPDATE fundraiser SET num_comments={} WHERE url='{}'".format(row['num_comments'],
                                                                                           row['url'].replace('\\',
                                                                                                              '\\\\')))

                db.commit()
            except:
                db.rollback()
                import pickle
                with open('failed_chunk_{}.json'.format(process_index), 'w') as f:
                    pickle.dump(chunk, f)
                with open('failed_comments_{}.json'.format(process_index), 'w') as f:
                    pickle.dump(comments, f)
                with open('failed_num_comments_data_{}.json'.format(process_index), 'w') as f:
                    pickle.dump(num_comments_data, f)

                raise


def test():
    url = 'www.crowdrise.com/bmc2014bostonmarathon/fundraiser/johndrachman'
    file_path = url
    with open('failed_chunk_1.json') as f:
        chunk = pickle.load(f)
    parser = etree.HTMLParser()
    try:
        f = open(file_path)
    except IOError:
        f = open(file_path + '.1')
    f_raw = f.read()
    f_re = re.sub("<!--.*?--!>", "", f_raw)  # kill these bad comments
    f_re = re.sub("<!--.*?-->", "", f_re)  # kill good comments
    f_removed_whitespace = f_re.translate(None, '\n\t\r')
    f_unicode = unicode(f_removed_whitespace, encoding='utf-8', errors='replace')
    try:
        tree = fromstring(f_removed_whitespace)
    except RuntimeError:
        logging.warning('RuntimeError on {} when trying to parse (stack overflow?)'.format(file_path))
        f_string = StringIO(f_unicode)
        tree = etree.parse(f_string, parser)
    except ValueError:
        logging.warning(
            'RuntimeError on {} when trying to parse (incompatible character?)'.format(file_path))
        f_string = StringIO(f_unicode)
        tree = etree.parse(f_string, parser)
    try:
        cur_comments = CrowdriseScraper.get_comments(tree, url)
    except:
        logging.error('Failure. url={}'.format(url))
        logging.error(traceback.format_exc())
    pass


def run(num_connections=4):
    with open('urls_to_scrape') as f:
        # urls_to_scrape = [dict(url=x[:-1].replace('https://', '')) for x in f.readlines()]
        urls_to_scrape = set(x[:-1].replace('https://', '') for x in f.readlines())
    db = db_connections.get_fungrosencrantz_schema('crowdrise')
    db['tmp'].insert_many([dict(url=x) for x in urls_to_scrape])
    q = """
SELECT
    t1.*
FROM
    crowdrise.tmp
        JOIN
    (SELECT
        url, file_path
    FROM
        `event` UNION ALL SELECT
        url, file_path
    FROM
        `charity` UNION ALL SELECT
        url, file_path
    FROM
        `fundraiser` UNION ALL SELECT
        url, file_path
    FROM
        `special_user` UNION ALL SELECT
        url, file_path
    FROM
        `user`) AS t1 ON tmp.url = t1.url
;
    """
    file_path_of_existing_urls = [x for x in db.query(q)]
    db['tmp'].drop()
    new_urls = urls_to_scrape - set(x['url'] for x in file_path_of_existing_urls)
    all_downloads = file_path_of_existing_urls + [dict(url=x, file_path=x + '.1') for x in new_urls]
    with open('newest_files.json', 'w') as f:
        json.dump(all_downloads, f)
    all_downloads = [dict(url=x, file_path=x + '.1') for x in new_urls]
    sitemap_downloader.create_directory_structure_for_url_list([x['url'] for x in all_downloads])
    # delete previous log files
    for f_name in ['wget_{}.log'.format(x) for x in range(num_connections)]:
        if os.path.isfile(f_name):
            os.remove(f_name)
    for chunk in split_array_into_chunks(all_downloads, chunk_size=num_connections):
        processes = []
        for p_index, url_dict in enumerate(chunk):
            url = url_dict['url']
            path = url_dict['file_path']
            processes.append(subprocess.Popen(
                'wget --append-output {} -O {} {}'.format(
                    'wget_{}.log'.format(p_index), path, 'https://' + url), shell=True))

        for p in processes:
            p.wait()

    # with open('newest_files.json', 'w') as f:
    #    json.dump(all_downloads, f)

    # TODO: remove this when everything works
    with open('newest_files2.json', 'w') as f:
        json.dump(all_downloads, f)


def scrape_newest_files():
    db = db_connections.get_fungrosencrantz_schema('crowdrise')
    with open('newest_files.json') as f:
        newest_files_json = json.load(f)

    scraper = CrowdriseScraper()
    chunk_size = 10000
    for chunk_index, newest_files_json_chunk in enumerate(split_array_into_chunks(newest_files_json, chunk_size=10000)):
        for inner_index, row in enumerate(newest_files_json_chunk):
            url = row['url']
            path = row['file_path']
            scraper.scrape_file(path, file_index=chunk_index * chunk_size + inner_index)
        scraper.upload_crowdrise_data(db)


def download_urls_to_update(outfile='urls_to_scrape', db=db_connections.get_fungrosencrantz_schema('crowdrise')):
    last_scrape_time = db.query('''select max(last_scrape) as last_scrape from html;''').next()['last_scrape']
    last_scrape_time = last_scrape_time.replace(tzinfo=pytz.UTC)
    sitemap_urls = get_sitemap_urls()

    sitemap_df = pd.DataFrame([dict(url=x, category=x.split('/')[-2], num=int(x.split('/')[-1])) for x in sitemap_urls])
    grouped = sitemap_df.groupby('category')
    urls_to_scrape = []
    for category, urls_in_category in grouped:
        print(category)
        for index, series in urls_in_category.sort_values(['num'], ascending=False).iterrows():
            url = series['url']
            url_data = analyze_sitemap_xml(url)
            url_df = pd.DataFrame(url_data)
            if len(url_df) == 0:
                continue
            url_df = url_df.loc[(url_df['lastmod'] > last_scrape_time)]
            urls_to_scrape += list(url_df['loc'])

    cleaned_urls = []
    for url_index, url in enumerate(urls_to_scrape):
        url = url.replace('http://', 'https://')
        fundraiser_loc = url.find('/fundraiser/')

        if fundraiser_loc != -1:
            url = url[:fundraiser_loc]

        file_path = url.replace('https://', '')

        # check if the path has a folder in it
        # if it does, ignore since these are only special pages
        # also ignore any files that already exist
        if os.path.split(file_path)[0] == 'www.crowdrise.com':
            cleaned_urls.append(url)

    cleaned_urls = set(cleaned_urls)

    with open(outfile, 'w') as f:
        for url in cleaned_urls:
            f.write(url + '\n')

    return cleaned_urls


def update_html_table_from_url_file(infile, db, process_num, total_processes):
    '''
    with open("../lib/fungrosencrantz_login", 'r') as f:
        login = json.load(f)
    db = MySQLdb.connect(host=login['hostname'],
                         user=login['username'],
                         passwd=login['password'],
                         db='crowdrise',
                         use_unicode=True,
                         charset='utf8')
    c = db.cursor()
    c.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle
    c.execute("SET CHARACTER SET utf8mb4;")  # same as above
    c.execute("SET character_set_connection=utf8mb4;")  # same as above
    '''
    with open(infile) as f:
        urls = list(set([x[:-1] for x in f.readlines()]))
    urls = split_list(urls, total_processes)[process_num]

    for chunk in split_array_into_chunks(data=urls, chunk_size=10):
        db.begin()
        for url in chunk:
            r = requests.get(url, headers=REQUEST_HEADERS)
            assert str(comment_request.status_code)[0] == '2'
            row = dict(url=url.replace('https://', ''),
                       last_scrape=datetime.datetime.now().isoformat(),
                       html=r.content)
            db['html'].upsert(row=row, keys=['url'])
        db.commit()


# TODO: add the following urls to the database:
# NEVERMIND, urls point to equivalent projects
'''
SELECT project as 'url' FROM user_project
left join fundraiser on user_project.project = fundraiser.url
where fundraiser.url is null
group by project;
'''
# if __name__ == '__main__':
if False:
    TOTAL_PROCESSES = 8
    t0 = time.time()
    try:
        process_index = int(sys.argv[1])
    except IndexError:
        process_index = -1
    log_file = '{}.log'.format(sys.argv[0][:-3] if process_index == -1 else sys.argv[0][:-3] + '_' + str(process_index))
    if os.path.exists(log_file):
        os.remove(log_file)
    logging.basicConfig(level=logging.DEBUG, filename=log_file)
    logging.debug("Starting {}, process #{}.".format(sys.argv[0], process_index))
    try:
        db = db_connections.get_fungrosencrantz_schema('crowdrise')

        # update_fundraiser_donations(num_workers=TOTAL_PROCESSES)
        # urls = download_urls_to_update(outfile='urls_to_scrape', db=db)
        # for f in get_file_names_from_url_file(infile='urls_to_scrape'):
        #    if os.path.exists(f):
        #        os.remove(f)
        # sitemap_downloader.wget_urls('urls_to_scrape', num_processes=4)
        # insert_all_files_into_db(infile='urls_to_scrape', db=db)

        if len(sys.argv) == 1:  # no args
            processes = []
            for p_index in range(TOTAL_PROCESSES):
                processes.append(subprocess.Popen('python crowdrise_scraper.py {}'.format(p_index), shell=True))
            try:
                for p in processes:
                    p.wait()
            except:
                for p in processes:
                    logging.warning('killing process {}'.format(p.pid))
                    p.kill()
                raise
        else:
            update_html_table_from_url_file(infile='urls_to_scrape', db=db, process_num=0,
                                            total_processes=TOTAL_PROCESSES)
            # scrape_all_files(process_num=process_index, total_processes=TOTAL_PROCESSES, infile='E:\\urls_to_scrape', db=db)

        '''

        # add all files to trend table
        db['updated_urls'].insert_many([dict(url=x) for x in get_file_names_from_url_file('urls_to_scrape')],
                                       ensure=True)
        '''

        # run(num_connections=4)
        # scrape_newest_files()

        # sitemap_downloader.create_directory_structure_for_urls('urls_to_scrape2')
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        raise

    logging.info('{}, process #{} has completed in {} seconds.'.format(sys.argv[0], process_index, time.time() - t0))
