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
KEY_SHOW_OPENED = 'show_opened'
KEY_SHOW_UNPAID = 'show_unpaid'
KEY_SHOW_IRREGULAR = 'show_irregular'

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_API_KEY,
    KEY_API_SECRET,
    KEY_ESTABLISHMENT_ID,
    KEY_START_DATE,
    KEY_END_DATE,
    KEY_SHOW_OPENED,
    KEY_SHOW_UNPAID,
    KEY_SHOW_IRREGULAR
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
        api_key = params.get(KEY_API_KEY)
        api_secret = params.get(KEY_API_SECRET)
        start_date = params.get(KEY_START_DATE)
        end_date = params.get(KEY_END_DATE)
        start_date_parsed = dateparser.parse(start_date).strftime('%Y-%m-%d')
        end_date_parsed = dateparser.parse(end_date).strftime('%Y-%m-%d')
        show_opened = params.get(KEY_SHOW_OPENED)
        show_unpaid = params.get(KEY_SHOW_UNPAID)
        show_irregular = params.get(KEY_SHOW_IRREGULAR)

        # Request params
        request_url = urllib.parse.urljoin(
            url, 'reports/sales_summary/json/')
        logging.info(f'request_url - {request_url}')
        request_params = {
            'limit': 20,
            # 'show_opened': 1,
            # 'show_unpaid': 1,
            # 'show_irregular': 1,
            'range_from': f'{start_date_parsed}T00:00:00',
            'range_to': f'{end_date_parsed}T00:00:00'
        }

        if show_opened:
            request_params['show_opened'] = 1
        if show_unpaid:
            request_params['show_unpaid'] = 1
        if show_irregular:
            request_params['show_irregular'] = 1

        '''
        request_headers = {
            'Accept': 'application/json',
            'API-AUTHENTICATION': f'{api_key}:{api_secret}',
            'Cookie': 'csrftoken=GnFV46R3djUM3BL1bdSEU4TVHSTLTroZhtADVrNM2u6W0fCo6rg1RR7CzWoCk2N9; \
                sessionid=bc9ewx8l87fjga4n7nq7hinc4ko76tf5'
        }
        '''

        api_authentication = api_key+':'+api_secret

        request_headers = {
            'API-AUTHENTICATION': api_authentication
        }

        logging.info(f'request headers: {request_headers}')
        # Establishments
        establishment_id = params.get(KEY_ESTABLISHMENT_ID).replace(' ', '')
        establishment_id_array = establishment_id.split(',')

        # output
        mappings, headers = self.get_header()
        full_headers = headers
        # additional paramters for the output
        full_headers.append('establishment_id')
        full_headers.append('range_from')
        full_headers.append('range_to')
        # writer prep
        f = open(f'{self.tables_out_path}/sales_summary.csv', 'w')
        data_writer = csv.DictWriter(f, fieldnames=full_headers)
        data_writer.writeheader()
        # no_data_bool = True

        for e_id in establishment_id_array:

            # Pagination parameters
            data_length = 20
            offset_param = 0

            while data_length >= 20:

                logging.info(
                    f'Processing establishment [{e_id}] - offset [{offset_param}]')

                # request parameters

                tmp_param = request_params
                tmp_param['offset'] = offset_param
                tmp_param['establishment'] = int(e_id)

                logging.info(f'request_param: {tmp_param}')

                # GET Request
                data_in = self.get_request(
                    url=request_url, params=tmp_param, headers=request_headers)

                # Parsing mapping and outputting the row
                if data_in:
                    additional_columns = {
                        'establishment_id': e_id,
                        'range_from': tmp_param['range_from'],
                        'range_to': tmp_param['range_to']
                    }

                    self.parse_mapping(
                        data_writer=data_writer,
                        data_in=data_in,
                        mapping=mappings,
                        user_columns=additional_columns
                    )

                    # Pagination parameters
                    data_length = len(data_in)
                    offset_param += 20

                    # no_data_bool = False

                else:
                    data_length = 0

        f.close()
        '''
        if no_data_bool:
            os.remove(f'{self.tables_out_path}/sales_summary.csv')
            logging.info('No data to output.')'''

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

    def get_request(self, url, params, headers):

        response = requests.get(url, params=params, headers=headers)

        if response.status_code not in (200, 201):
            logging.warning(f'{response.status_code}')

        try:
            return response.json()
        except Exception:
            return None

    def parse_mapping(self, data_writer, data_in, mapping, user_columns):

        establishment_id = user_columns['establishment_id']

        for row in data_in:

            tmp_data = {}

            try:
                for i in mapping['sales_summary']:
                    tmp_data[mapping['sales_summary'][i]] = row.get(i)

                # adding esablishment id
                # tmp_data['establishment_id'] = establishment_id
                for col in user_columns:
                    tmp_data[col] = user_columns[col]

                # writer data
                data_writer.writerow(tmp_data)

            except Exception:
                logging.warning(f'Establish id [{establishment_id}] - {row}')


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
