import sys

if '../' not in sys.path:
    sys.path.insert(0, '../')

import os
import json
import logging
import db_connections
import shutil
from kiva_json_analyzer import analyze_loans_lenders_data, analyze_loans_data, analyze_lenders_data


def run():
    db = db_connections.get_fungrosencrantz_schema(schema='Kiva')
    for category in ('lenders', 'loans_lenders', 'loans'):
        for root, dirs, files in os.walk('to_be_analyzed/' + category):
            for i, file_name in enumerate(files):
                if file_name.endswith(".json"):
                    cur_file = os.path.join(root, file_name)
                    logging.info('Current file: ' + cur_file)
                    with open(cur_file) as f:
                        cur_file_json = json.load(f)
                    cur_file_list = cur_file_json[category]
                    if category == 'loans_lenders':
                        data = analyze_loans_lenders_data(cur_file_list)
                    elif category == 'loans':
                        data = analyze_loans_data(cur_file_list, scrape_time=cur_file_json['header']['date'])
                    elif category == 'lenders':
                        data = analyze_lenders_data(cur_file_list)
                    else:
                        raise Exception("Unknown category")
                    db_connections.multi_table_upload(data=data, db=db, update=True, strict=True)
                    shutil.move(src=cur_file, dst=cur_file.replace('to_be_analyzed/', 'done_analyzing/'))
                    # if i > -1: break  # only want to deal with one for now
                print("{}% DONE".format(100. * (i + 1) / len(files)))


if __name__ == "__main__":
    logging.basicConfig(level='INFO')
    import time

    t0 = time.time()
    run()
    print(time.time() - t0)
