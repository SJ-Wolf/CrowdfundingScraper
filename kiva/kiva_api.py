import sys
# if '../' not in sys.path:
#     sys.path.insert(0, '../')
import time
import requests
import logging
import json
import useful_functions
from . import kiva_json_analyzer
import datetime
import traceback


class KivaAPI:
    def __init__(self, api_id='edu.berkeley.haas.crowdfunding.kiva'):
        self._app_id = api_id
        self._first_api_request_time = None
        self._first_specific_api_request_time = None
        self._x_ratelimit_reset_time = 60

    def make_api_request(self, *args, **kwargs):
        """
        Makes a request to the Kiva API, trying to take into account their limits on the number of requests that can be
        made per minute.

        :param args: These are passed directly to requests.get()
        :param kwargs: These are passed directly to requests.get()
        :return: response from requests.get()
        """
        time.sleep(0.05)
        try:
            # add the app_id to the request
            if 'params' not in kwargs:
                kwargs['params'] = dict()
            kwargs['params']['app_id'] = self._app_id

            if self._first_api_request_time is None:
                self._first_api_request_time = time.time()

            # keep trying until we get a response that is not rejected due to rate limit restrictions
            # ideally we shouldn't hit these limits, but sometimes it happens
            # if it does, they got away after 10 minutes or so
            max_tries = 4
            current_try = 1
            while True:
                r = requests.get(*args, **kwargs)
                if r.status_code == 403 and 'org.kiva.RateLimitExceeded' in r.text:
                    logging.warning(
                        'Hit X-RateLimit. Waiting 10 minutes before retrying ({}/{} tries remaining).'.format(
                            max_tries - current_try, max_tries
                        ))
                    time.sleep(10 * 60)  # wait 10 minutes
                elif r.status_code == 502:
                    logging.warning(
                        '502 Error. Waiting 10 minutes before retrying ({}/{} tries remaining).'.format(
                            max_tries - current_try, max_tries
                        ))
                else:
                    break
                if current_try >= 4:
                    logging.error('Max retries reached. Exiting...')
                    raise Exception('Max retries reached.')
                current_try += 1

            # how many overall requests can be made per minute
            overall_remaining = int(r.headers['X-RateLimit-Overall-Remaining'])

            # some API calls have their own limit, which is on a separate timer
            specific_remaining = r.headers.get('X-RateLimit-Specific-Remaining')

            # remember the time if this is the first time seeing a specific request this minute
            if specific_remaining is not None and self._first_specific_api_request_time is None:
                self._first_specific_api_request_time = time.time()

            # mark that we're no longer restricted by specific requests
            if specific_remaining is None and self._first_specific_api_request_time is not None:
                self._first_specific_api_request_time = None

            logging.debug(
                'Requests remaining:  overall={}, specific={}'.format(overall_remaining, specific_remaining))

            # if we've hit either the general or specific limit, wait an appropriate amount of time
            if overall_remaining <= 1 or (specific_remaining is not None and int(specific_remaining) <= 1):
                if specific_remaining is not None and int(specific_remaining) <= 1:
                    wait_time = self._x_ratelimit_reset_time
                else:
                    wait_time = self._first_api_request_time + self._x_ratelimit_reset_time - time.time() + 1
                if wait_time < 0:
                    wait_time = self._x_ratelimit_reset_time
                logging.info('Waiting {} seconds for more requests'.format(wait_time))
                time.sleep(wait_time)
                self._first_api_request_time = time.time()
            return r
        except:
            print(r.headers)
            raise

    def get_most_recent_loans(self, stop_loan_id, per_page=500, ids_only=False):
        """

        :param ids_only: If the function should only return the ids of the loans, not any data
        :param stop_loan_id: Stop finding new loans when this loan_id is seen
        :param per_page: How many loans should be downloaded per request
        :return: A list of dictionaries, each dictionary describing a loan
        """
        page = 1
        r = self.make_api_request(url='https://api.kivaws.org/v1/loans/search.json',
                                  params=dict(page=page, per_page=per_page, ids_only='true' if ids_only else 'false'))
        newest_loans_json = json.loads(r.text)['loans']
        all_loan_json = []
        all_loan_json += newest_loans_json

        if ids_only:
            while stop_loan_id not in newest_loans_json:
                page += 1
                r = self.make_api_request(url='https://api.kivaws.org/v1/loans/search.json',
                                          params=dict(page=page, per_page=per_page, ids_only="true"))
                newest_loans_json = json.loads(r.text)['loans']
                all_loan_json += newest_loans_json
        else:
            while stop_loan_id not in [x['id'] for x in newest_loans_json]:
                page += 1
                r = self.make_api_request(url='https://api.kivaws.org/v1/loans/search.json',
                                          params=dict(page=page, per_page=per_page, ids_only="false"))
                newest_loans_json = json.loads(r.text)['loans']
                all_loan_json += newest_loans_json

        return all_loan_json

    def get_loans_lenders(self, loan_id):
        """

        :param loan_id: loan to get lenders for
        :return: list of the lender ids associated with the loan
        """
        loans_lenders = []
        r = self.make_api_request(url='https://api.kivaws.org/v1/loans/{}/lenders.json'.format(loan_id),
                                  params=dict(ids_only='true', page=1))
        response_json = json.loads(r.text)
        try:
            num_pages = response_json['paging']['pages']
        except:
            with open('output.text', 'w') as f:
                f.write(r.text)
            raise
        loans_lenders += response_json['lenders']

        for page in range(2, num_pages + 1):
            r = self.make_api_request(url='https://api.kivaws.org/v1/loans/{}/lenders.json'.format(loan_id),
                                      params=dict(ids_only='true', page=page))
            response_json = json.loads(r.text)
            loans_lenders += response_json['lenders']
        return loans_lenders

    def get_lender_data(self, lender_ids):
        """

        :param lender_ids: list of lender ids to get detailed information on
        :return: list of dictionaries describing each lender
        """
        assert len(lender_ids) <= 50
        if len(lender_ids) == 0:
            return dict(lender=[])
        r = self.make_api_request(url='https://api.kivaws.org/v1/lenders/{}.json'.format(
            ",".join([lender for lender in lender_ids])))
        if r.status_code == 404:  # all lender_ids are missing
            lender_data = dict()
            lender_data['lender'] = [dict(lender_id=x) for x in lender_ids]
        else:
            lender_json = json.loads(r.text)['lenders']
            lender_data = kiva_json_analyzer.analyze_lenders_data(lender_json)
        # add any lenders that couldn't be found
        existing_lender_ids = set([x['lender_id'] for x in lender_data['lender']])
        lenders_to_add = set(lender_ids) - existing_lender_ids
        lender_data['lender'] += [dict(lender_id=x) for x in lenders_to_add]
        return lender_data

    def get_detailed_loan_data(self, loans_json, scrape_time=str(datetime.datetime.now())):
        """

        :param loans_json: list of loan dictionaries
        :param scrape_time: this should be the time the loan data was scraped, default is now
        :return: a list of loan dictionaries, with more details
        """
        loan_ids = [x['id'] for x in loans_json]
        assert len(loan_ids) <= 100
        r = self.make_api_request(url='https://api.kivaws.org/v1/loans/{}.json'.format(
            ",".join([str(num) for num in loan_ids])))
        loan_data = json.loads(r.text)
        detailed_loan_data = loan_data['loans']
        for row_index in range(len(detailed_loan_data)):
            keys = detailed_loan_data[row_index].keys()
            for key in keys:
                if detailed_loan_data[row_index][key] is None:
                    del detailed_loan_data[row_index][key]
        combined_loan_data = useful_functions.combine_list_of_dicts_on_column(detailed_loan_data, loans_json, 'id')
        loan_data = kiva_json_analyzer.analyze_loans_data(loans=combined_loan_data, scrape_time=scrape_time)
        return loan_data

    def get_loans_lenders_data(self, loans_json):
        """

        :param loans_json: list of loan dictionaries
        :return: a dictionary with one key 'loan_lender' and the value is a list of dictionaries, where the keys are:
        id: the id number of the loan
        lender_id: the id number of the lender
        """
        loans_to_get_lenders = [x['id'] for x in loans_json if x['lender_count'] > 0]
        loans_lenders = []
        for loan_id in loans_to_get_lenders:
            loans_lenders.append(dict(
                id=loan_id,
                lender_ids=self.get_loans_lenders(loan_id=loan_id)))
        loans_lenders_data = kiva_json_analyzer.analyze_loans_lenders_data(loans_lenders)
        return loans_lenders_data


def download_all_ids():
    last_loan_id = 91
    api = KivaAPI()
    page = 1
    output_file = open('all_ids.txt', 'w')

    while True:  # keep making requests to new pages until last_loan_id is in the loans from the new page
        while True:  # keep making requests until the server is not under maintenance
            r = api.make_api_request(url='https://api.kivaws.org/v1/loans/search.json',
                                     params=dict(page=page, per_page=500, ids_only="true"))
            full_json = json.loads(r.text)
            if full_json.get('code') == 'org.kiva.ServerMaintenance':  # server maintenance
                time.sleep(30)
            else:
                newest_loans_json = json.loads(r.text)['loans']
                break

        for loan_id in newest_loans_json:
            output_file.write('{}\n'.format(loan_id))
        page += 1
        if last_loan_id in newest_loans_json:
            break
    output_file.close()


def run():
    last_loan_id = 91
    api = KivaAPI()
    page = 1

    while True:
        r = api.make_api_request(url='https://api.kivaws.org/v1/loans/search.json',
                                 params=dict(page=page, per_page=500, ids_only="false"))
        newest_loans_json = json.loads(r.text)['loans']
        with open('to_be_analyzed/loans/{}.json'.format(page), 'w') as f:
            json.dump(newest_loans_json, f)
        page += 1
        if last_loan_id in [x['id'] for x in newest_loans_json]:
            break


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting {}.".format(sys.argv[0]))
    try:
        run()
    except Exception:
        logging.error(traceback.format_exc())
        logging.info('A fatal error has occurred.')
        raise
    logging.info('{} has completed.'.format(sys.argv[0]))
