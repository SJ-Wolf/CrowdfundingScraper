import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')
from lxml import etree
from io import StringIO, BytesIO
import pprint
from lxml.html.soupparser import fromstring
import re


def find_unique_elements():
    files = ['www.crowdrise.com/spartan',  # special
             'www.crowdrise.com/nursesgiveback',  # fundraiser
             'www.crowdrise.com/celebratingrobinwilliams',  # user
             'www.crowdrise.com/woundedwarriorproject',  # charity
             'www.crowdrise.com/toughmuddertri-state',  # event
             ]

    trees = []

    parser = etree.HTMLParser()

    for f_name in files:
        with open(f_name, 'rb') as f:
            f_read = unicode(object=f.read(), encoding='utf-8', errors='replace')
            f_string = StringIO(f_read)
            trees.append(etree.parse(f_string, parser))

    elements_unique = [set([x.attrib['id'] for x in tree.xpath('//*[@id]')]) for tree in trees]

    for inner_index, inner_f_name, inner_tree in zip(range(len(files)), files, trees):
        for outer_index, outer_f_name, outer_tree in zip(range(len(files)), files, trees):
            if outer_index < inner_index:
                ids_of_comparison_page = set([x.attrib['id'] for x in outer_tree.xpath('//*[@id]')])
                elements_unique[inner_index] -= ids_of_comparison_page
            if outer_index == inner_index:
                continue
                ids_of_current_page = set([x.attrib['id'] for x in inner_tree.xpath('//*[@id]')])
                elements_unique[inner_index] = ids_of_current_page
            if outer_index > inner_index:
                ids_of_comparison_page = set([x.attrib['id'] for x in outer_tree.xpath('//*[@id]')])
                elements_unique[inner_index] -= ids_of_comparison_page

    pprint.pprint(elements_unique)


def run():
    f_name = 'www.crowdrise.com/janetstark'
    parser = etree.HTMLParser()
    with open(f_name, 'r') as f:
        f_raw = f.read()
        f_re = re.sub("<!--.*?--!>", "", f_raw)
        f_removed_whitespace = f_re.translate(None, '\n\t\r')
        # tree = fromstring(f_removed_whitespace)
        f_unicode = unicode(f_removed_whitespace, encoding='utf-8', errors='replace')
        f_string = StringIO(f_unicode)
        tree = etree.parse(f_string, parser)

    # from crowdrise_scraper import *

    with open('static_pages.txt') as f:
        static_pages = set(f.read().split('\n'))
    true_url = tree.xpath('//meta[@property="og:url"]')[0].attrib['content'].replace('https://', '').replace('http://', '')
    if true_url[-1] == '/':
        true_url = true_url[:-1]

    print
    true_url


def get_comments(tree):
    import timestring
    def get_comments_from_tree(comment_tree):
        comments = []
        for comment in comment_tree.xpath('//div[@class="container"]/div[@class="full"]//div[@class="title fLeft"]'):
            comment_row = dict()
            try:
                comment_row['name'] = comment.xpath('.//h4/text()')[0].strip()
            except IndexError:
                comment_row['name'] = None
            try:
                comment_row['amount'] = comment.xpath('.//h5/text()')[0].strip().replace('DONATION: ', '')
            except IndexError:
                comment_row['amount'] = None
            try:
                comment_row['date'] = timestring.Date(comment.xpath('.//i/text()')[0].strip())
            except IndexError:
                comment_row['date'] = None
            try:
                comment_row['id'] = int(comment.xpath('..')[0].attrib['id'].replace('comment_', ''))
            except IndexError:
                comment_row['id'] = None

            comments.append(comment_row)
        return comments

    try:
        additional_comments_url = tree.xpath('//*[@id="donorComments"]//a[@class="more"]')[0].attrib['href']
    except IndexError:
        additional_comments_url = None

    if additional_comments_url is None:  # scrape comments in current tree
        donations = get_comments_from_tree(tree)
    else:  # scrape comments in new url(s)
        donations = []
        cur_comment_page = 0
        cur_additional_comments_url = additional_comments_url
        while True:
            comment_request = requests.get('https://www.crowdrise.com' + cur_additional_comments_url)
            comment_tree = fromstring(comment_request.text)
            donations += get_comments_from_tree(comment_tree)
            if len(comment_tree.xpath('//*[@id="seeMoreDonations"]')) == 1:
                cur_comment_page += 1
                cur_additional_comments_url = additional_comments_url + '/' + str(cur_comment_page)
            else:
                break
    return donations


if __name__ == '__main__':
    import subprocess
    import os
    import time

    url = r'E:\www.crowdrise.com\general-donations199'.lower()
    # filename = 'www.crowdrise.com/uweza-36331'
    # print os.path.getmtime(filename), time.gmtime(os.path.getmtime(filename))
    # subprocess.call('wget -O {} {}'.format(filename, url), shell=True)
    # print os.path.getmtime(filename), time.gmtime(os.path.getmtime(filename))

    from lxml import etree
    import requests

    # parser = etree.HTMLParser()
    # r = requests.get('https://' + url)
    # f_string = StringIO(r.text)
    # tree = etree.parse(source=f_string, parser=parser)
    # print(tree.xpath('//*[@id="myFundraisersContainer"]'))
    # run()
    # tree = fromstring(r.text)

    import crowdrise_scraper

    scraper = crowdrise_scraper.CrowdriseScraper()
    scraper.scrape_file(cur_file_name=url, file_index=0)
    pprint.pprint(scraper.crowdrise_data)

    pass
