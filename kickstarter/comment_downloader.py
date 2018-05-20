import json
import pprint
import time
import db_connections
import webpageDownloader
import lxml
import logging


def onlyNumerics(seq):
    return filter(type(seq).isdigit, seq.split(".")[0])


def comment_scraper(tree, projectid):
    comments = []
    comment_sections = tree.xpath('//li[@class="page"]/ol[@class="comments"]/li')

    for c_section in comment_sections:
        comment = dict()
        comment['projectid'] = projectid
        comment['id'] = onlyNumerics(c_section.attrib['id'])
        user_section = c_section.xpath('.//a[contains(@class, "author")]')
        assert len(user_section) == 1
        comment['user_id'] = user_section[0].attrib['href'].replace("/profile/", '')
        comment['user_name'] = user_section[0].text
        comment['body'] = "\n".join(c_section.xpath('.//p/text()'))
        comment['by_creator'] = (c_section.attrib['class'].find("creator") != -1)
        comment['post_date'] = c_section.xpath('.//data[@itemprop="Comment[created_at]"]')[0].attrib['data-value'].split("T")[0].replace('"', '')

        if comment['user_id'] == "":
            raise Exception("Blank user_id")
        comments.append(comment)
    return comments


def upload_comments_from_urls(ids_for_url=dict(), workers=4, verbose_level=0, existing_comment_ids=set()):
    """

    :param ids_for_url: a dictionary where the keys are base project urls and the values are the corresponding
            projectids. If this is None, then the function will resume from where it left off (assuming it crashed and
            was able to make a dump to disk)
    :param workers: maximum number of http requests that can be sent at once
    :param verbose_level: how verbose the function should be, up to 3
    :param existing_comment_ids: a set of existing comment ids; function stops uploading comments once a comment
            has an id in existing_comment_ids
    :return: None
    """

    def upload_comments(c):
        if verbose_level > 1:
            logging.debug("Uploading {0} comments".format(len(c)))
        db_connections.uploadOutputFile(c, kickstarter_db, table='comments')

    # initialize variables
    if ids_for_url is None:
        # get urls, comments, and completed_comments from disk
        with open('comment_uploader_mem_dump.json', 'rb') as f:
            mem_dump = json.load(f)
            urls = mem_dump[0]
            comments = mem_dump[1]
            completed_comments = mem_dump[2]
            ids_for_url = mem_dump[3]
            existing_comment_ids = mem_dump[4]
            existing_comment_ids = set(existing_comment_ids)
    else:
        # normal setup
        urls = ids_for_url.keys()
        comments = dict()

        for url in urls:
            # initialize the comment lists
            comments[url] = []
            # make sure the urls do not end in /, /comments, etc.
            if len(url.split("/")) != 6:
                raise Exception("Unexpected format of url: {0}".format(url))
        completed_comments = []

    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter', traditional=True)
    try:
        while len(urls) > 0:
            html_responses, num_passes = webpageDownloader.get_html_from_urls(
                urls, append='/comments', verbose_level=verbose_level, workers=workers
            )
            urls = []
            for url_index, url in enumerate(html_responses):
                try:
                    html = html_responses[url]
                    url = url.split('?')[0].replace('/comments', '')
                    assert html is not None
                    if type(html) == str:
                        html = html.decode('utf-8')
                    tree = lxml.html.fromstring(html)
                    new_comments = comment_scraper(tree, ids_for_url[url])
                    if len(new_comments) > 0:
                        found_existing_comment = int(new_comments[0]['id']) in existing_comment_ids
                    else:
                        found_existing_comment = False
                    if found_existing_comment and verbose_level > 1:
                        logging.debug("Found existing comment")
                    comments[url] += new_comments
                    load_more_section = tree.xpath('//a[contains(@class, "older_comments")]')
                    if len(load_more_section) == 0 or found_existing_comment:
                        completed_comments += comments[url]
                        del comments[url]
                    elif len(load_more_section) == 1:
                        href = load_more_section[0].attrib['href']
                        href = href.split('?')[1]
                        url = url + "/comments?" + href
                        urls.append(url)
                    else:
                        raise Exception("Too many load more sections")

                    if len(completed_comments) > 1000:
                        upload_comments(completed_comments)
                        completed_comments = []

                    # reconnect to the database to delete temporary tables
                    if url_index > 0 and url_index % 50 == 0:
                        kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter', traditional=True)
                except:
                    logging.debug("Last url: " + url)
                    raise
        if len(completed_comments) > 0:
            upload_comments(completed_comments)
    except:
        logging.error("Error occurred. Dumping partial upload to file.")
        existing_comment_ids = list(existing_comment_ids)
        with open('comment_uploader_mem_dump.json', 'wb') as f:
            json.dump([urls, comments, completed_comments, ids_for_url, existing_comment_ids], f)
        logging.error("Dump complete.")
        raise


def update_comments(update_type=0):
    """
    :param update_type: 0 - update comments of live projects\n
                        1 - find comments where comment_count > 0 but no comments in the comments table.
                            This is useful if projects that are no longer live have been added\n
                        2 - update comments from all projects where comment_count > 0
                        3 - finish failed upload chunk. Used to make sure all comments are uploaded from projects after
                            a failed upload. Original update type needs to be run again.
    :return:
    """
    if update_type == 3:
        upload_comments_from_urls(ids_for_url=None, verbose_level=2)
        return

    if update_type == 0:
        query_restrictions = """ from project where status = "live" """
    elif update_type == 1:
        query_restrictions = """ from project left join comments on project.id = comments.projectid
                                where comments.projectid is Null and comment_count > 0 """
    elif update_type == 2:
        query_restrictions = """ from project where comment_count > 0 """
    else:
        raise Exception("Unsupported update type: {0}".format(update_type))

    db = db_connections.get_fungrosencrantz_schema('kickstarter', traditional=True)

    # Don't bother finding the existing comment ids if only the comments from projects that haven't had their comments
    # downloaded yet will be downloaded
    if update_type == 1:
        existing_comment_ids = set()
    elif update_type in (0, 2):
        logging.debug("Getting existing comments")
        t1 = time.time()
        existing_comment_ids = set()
        if update_type == 0:
            q = "select comments.id from comments join project on comments.projectid = project.id where status = 'live'"
        elif update_type == 2:
            q = "select comments.id from comments join project on comments.projectid = project.id where comment_count > 0"
        for x in db.query(q):
            existing_comment_ids.add(x['id'])
        logging.debug("Download took {0} seconds".format(time.time() - t1))

    logging.debug("Getting projects to get comments for")

    chunk_size = 100
    offset = 0
    same_loop_count = 0
    prev_last_urls = set()
    while True:
        db = db_connections.get_fungrosencrantz_schema('kickstarter', traditional=True)
        # create the appropriate query based on the update type
        q = "select project.id as projectid, project.url as url {0} order by comment_count desc limit {1}, {2}".format(
            query_restrictions, offset, chunk_size
        )
        result = db.query(q)

        # Since update type 1 skips projects that have comments already in the comments table, the query is different
        # each time. Other update options return the same projects each time, so an there has to be an offset
        if update_type != 1:
            offset += chunk_size

        # convert the results from this query to a dictionary, ids_for_urls
        ids_for_urls = dict()
        for row in result:
            ids_for_urls[row['url']] = row['projectid']

        # done updating comments once the query returns no results (in the case of update type 0 or 2)
        if len(ids_for_urls) == 0:
            break

        # try to upload the comments from the projects in ids_for_urls
        try:
            upload_comments_from_urls(ids_for_urls, workers=10, verbose_level=2, existing_comment_ids=existing_comment_ids)
        except:
            logging.debug(pprint.pformat(ids_for_urls.keys()))
            raise

        # if the update type is 1, there may be projects that have no comments on the comments page, even though
        # they are supposed to. If the query is the same 4 times in a row, then assume that no more comments can be
        # uploaded
        if update_type == 1:
            last_urls = set(ids_for_urls.keys())
            if prev_last_urls == last_urls:
                same_loop_count += 1
            if same_loop_count > 2:
                if update_type == 1:
                    logging.debug("Finished because remaining projects will not download (probably don't have any comments)")
                    break
            prev_last_urls = last_urls


if __name__ == "__main__":
    import os

    log_file = 'comment_downloader.log'
    if os.path.exists(log_file):
        os.remove(log_file)
    logging.basicConfig(filename=log_file, level=logging.INFO)
    # upload_comments_from_urls(ids_for_url=None, verbose_level=2)
    update_comments(0)
    update_comments(1)
