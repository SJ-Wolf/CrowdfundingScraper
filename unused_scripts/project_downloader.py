# this module takes all the urls in the project database and downloads their html into a local database
# only useful if the project database has already been populated

from unused_scripts import webpageDownloader, db_connections
import sys


# imports backers with 50 or fewer backers since these don't require javascript to scroll down
def download_small_backers(workers):
    html_db = db_connections.get_intermediate_db()
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    # get the urls and their corresponding ids to download
    ids_for_base_urls = db_connections.get_ids_for_base_urls(
        db=kickstarter_db, table_name='project', query_append='and backers_count <= 50 and backers_count > 0',
        scrape_table_name='backer', scrape_table_column_name='projectid')
    webpageDownloader.urls_to_database(
        ids_for_base_urls=ids_for_base_urls, db_connector=db_connections.get_intermediate_db,
        html_table_name='backer_html', url_append='/backers', verbose_level=2, auto_adjust_workers=False,
        max_workers=workers)


def download_main_pages(workers):
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    # get the urls and their corresponding ids to download
    ids_for_base_urls = db_connections.get_ids_for_base_urls(db=kickstarter_db, table_name='project')
    webpageDownloader.urls_to_database(
        ids_for_base_urls=ids_for_base_urls, db_connector=db_connections.get_intermediate_db,
        html_table_name='main_html', url_append='', verbose_level=2, auto_adjust_workers=False,
        max_workers=workers, chunk_size=104)


def download_description_pages(workers):
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    # get the urls and their corresponding ids to download
    ids_for_base_urls = db_connections.get_ids_for_base_urls(db=kickstarter_db, table_name='project')
    webpageDownloader.urls_to_database(
        ids_for_base_urls=ids_for_base_urls, db_connector=db_connections.get_intermediate_db,
        html_table_name='description_html', url_append='/description', verbose_level=2, auto_adjust_workers=False,
        max_workers=workers, chunk_size=104)


def download_update_pages(workers):
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    # get the urls and their corresponding ids to download
    ids_for_base_urls = db_connections.get_ids_for_base_urls(db=kickstarter_db, table_name='project')
    webpageDownloader.urls_to_database(
        ids_for_base_urls=ids_for_base_urls, db_connector=db_connections.get_intermediate_db,
        html_table_name='update_html', url_append='/updates', verbose_level=2, auto_adjust_workers=False,
        max_workers=workers, chunk_size=104)


def download_reward_pages(workers):
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    ids_for_base_urls = db_connections.get_ids_for_base_urls(
        db=kickstarter_db, table_name='project', query_append="",
        scrape_table_name='reward', scrape_table_column_name='projectid')
    webpageDownloader.urls_to_database(ids_for_base_urls, db_connections.get_intermediate_db, 'reward_html',
                                       url_append='/rewards', chunk_size=104, max_workers=workers, verbose_level=2)


def main():
    message = """
    /u downloads update pages
    /m downloads main pages
    /d downloads description pages
    /b downloads backers pages with less than or equal to 50 backers
    /r downloads reward pages
    Format: (/u, /m, /d, /b, /r)[, num_workers]"""
    if len(sys.argv) >= 2:
        if len(sys.argv) == 3:
            workers = int(sys.argv[2])
        else:
            workers = 8
        input = sys.argv[1]
        if input == "/u":  # update pages
            print
            "Downloading update pages."
            download_update_pages(workers)
        elif input == "/m":  # main pages
            print
            "Downloading main pages"
            download_main_pages(workers)
        elif input == "/d":  # description pages
            print
            "Downloading description pages"
            download_description_pages(workers)
        elif input == "/b":  # backer pages (<=50 backers)
            print
            "Downloading backers pages with less than or equal to 50 backers"
            download_small_backers(workers)
        elif input == "/r":  # reward pages
            print
            "Downloading rewards pages"
            download_reward_pages(workers)
        elif input == "/h" or input == "/help":  # help
            print
            message
        else:
            print
            "Invalid input"
            print
            message
    else:
        print
        "Please enter exactly one argument."


if __name__ == '__main__':
    # main()
    # functions must be changed for new webpageDownloader.urls_to_database code!
    pass
