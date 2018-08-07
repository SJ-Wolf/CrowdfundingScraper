import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')

import db_connections


def create_snapshot():
    """ takes a snapshot of the project table and puts it into the funding_trend table """
    db = db_connections.get_fungrosencrantz_schema('kickstarter', traditional=True)

    db.query("""
    replace into funding_trend (projectid, date_added, time_added, amount_pledged, backer_count, update_count, comment_count, `status`)
    select id, CURDATE(), CURTIME(), amount_pledged, backer_count, update_count, comment_count, `status`
    from project
    where CURDATE() <= end_date and CURDATE() >= start_date;
    """
             )


if __name__ == "__main__":
    create_snapshot()
