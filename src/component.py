'''
Template Component main class.

'''
import logging
import os
from pathlib import Path
import requests
import sys
import dateparser
import json
import csv
import urllib

from keboola.component import CommonInterface

# configuration variables
KEY_URL = 'url'
KEY_API_KEY = 'api_key'
KEY_API_SECRET = '#api_secret'
KEY_ESTABLISHMENT_ID = 'establishment_id'
KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_API_KEY,
    KEY_API_SECRET,
    KEY_ESTABLISHMENT_ID,
    KEY_START_DATE,
    KEY_END_DATE
]
REQUIRED_IMAGE_PARS = []

APP_VERSION = '0.0.1'


def get_local_data_path():
    return Path(__file__).resolve().parent.parent.joinpath('data').as_posix()


def get_data_folder_path():
    data_folder_path = None
    if not os.environ.get('KBC_DATADIR'):
        data_folder_path = get_local_data_path()
    return data_folder_path


class Component(CommonInterface):
    def __init__(self):
        # for easier local project setup
        data_folder_path = get_data_folder_path()
        super().__init__(data_folder_path=data_folder_path)

        try:
            # validation of required parameters. Produces ValueError
            self.validate_configuration(REQUIRED_PARAMETERS)
            self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

        if self.configuration.parameters.get(KEY_DEBUG):
            self.set_debug_mode()

    @staticmethod
    def set_debug_mode():
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters

        # Validate user input
        self.validate_user_input(params)

        # Request parameters
        url = params.get(KEY_URL)
        api_key = params.get(KEY_API_SECRET)
        api_secret = params.get(KEY_API_SECRET)
        start_date = params.get(KEY_START_DATE)
        end_date = params.get(KEY_END_DATE)

        # Request params
        params = {
            'api_key': api_key,
            'api_secret': api_secret,
            'limit': 20,
            'range_from': f'{start_date}T00:00:00',
            'range_to': f'{end_date}T00:00:00',
            'show_unpaid': 1,
            'show_irregular': 1
        }

        # Establishments
        establishment_id = params.get(KEY_ESTABLISHMENT_ID).replace(' ', '')
        establishment_id_array = establishment_id.split(',')

        # output
        mappings, headers = self.get_header()
        full_headers = headers.append('establishment_id')
        f = open(f'{self.table_out_path}/sales_summary.csv', 'w')
        data_writer = csv.DictWriter(f, fieldnames=full_headers)
        data_writer.writeheader()

        for e_id in establishment_id_array:

            # Pagination parameters
            data_length = 20
            offset_param = 0

            while data_length >= 20:

                # request parameters
                request_url = urllib.parse.urljoin(
                    url, 'reports/sales_summary/json')
                request_param = params
                request_param['offset'] = offset_param
                request_param['establishment'] = e_id

                # GET Request
                data_in = self.get_request(
                    url=request_url, params=request_param)

                # Parsing mapping and outputting the row
                self.parse_mapping(
                    data_writer=data_writer,
                    data_in=data_in,
                    mapping=mappings,
                    establishment_id=e_id
                )

                # Pagination parameters
                data_length = len(data_in)
                offset_param += 20

            data_in = self.fetch(url, params, establishment_id)

    def get_header(self):

        with open('src/mapping.json', 'r') as f:
            data_in = json.load(f)

        header = []
        for col in data_in['sales_summary']:
            header.append(data_in['sales_summary'][col])

        return data_in, header

    def validate_user_input(self, params):

        # 1 credentials
        if not params.get(KEY_API_KEY) or not params.get(KEY_API_SECRET) or not params.get(KEY_URL):
            logging.error('Credentials are missing.')
            sys.exit(1)

        # 2 establishment cannot be empty or nothing
        establishment_id_array = params.get(KEY_ESTABLISHMENT_ID).split(',')
        if len(establishment_id_array) < 1:
            logging.error('Please configure establishment ids')
            sys.exit(1)

        # 3 check start and end date
        if not params.get(KEY_START_DATE) or not params.get(KEY_END_DATE):
            logging.error('Please configure your dates')
            sys.exit(1)

        start_date = dateparser.parse(params.get(KEY_START_DATE))
        end_date = dateparser.parse(params.get(KEY_END_DATE))
        if start_date > end_date:
            logging.error('StartDate cannot be larger than EndDate')
            sys.exit(1)

    def get_request(self, url, params):

        response = requests.get(url, params=params)

        if response.status_code not in (200, 201):
            logging.warning(f'{response.status_code} - {response.text}')

        if 'sales_summary' in response.json():
            return response.json()['sales_summary']
        else:
            return []

    def parse_mapping(self, data_writer, data_in, mapping, establishment_id):

        for row in data_in:

            tmp_data = {}
            for i in mapping:
                tmp_data[mapping[i]] = row.get(i)

            # adding esablishment id
            tmp_data['establishment_id'] = establishment_id

            # writer data
            data_writer.writerow(tmp_data)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
