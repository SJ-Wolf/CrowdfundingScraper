import sqlite3

from utils.download_utils import get_url

with sqlite3.connect('file:../kickstarter.db?mode=ro', uri=True) as db:
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    cur.execute('select id from creator')
    user_ids = [x[0] for x in cur.fetchall()]
    for user_id in user_ids:
        url = f'https://www.kickstarter.com/profile/{user_id}/about'
        print(url)
        get_url(url)
        # try:
        #     get_url(url, wait_time=0)
        # except requests.HTTPError as e:
        #     if e.response.status_code == '503':
        #         with sqlite3.connect('html.db') as db2:
        #             cur = db2.cursor()
        #             cur.execute('insert into html values (?, ?)', (f'https://www.kickstarter.com/profile/{user_id}/about', None))
