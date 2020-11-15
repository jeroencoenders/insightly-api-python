"""API Helpers module"""

from time import sleep

import requests
from requests.auth import HTTPBasicAuth

from .settings import PAGESIZE

def build_url(*args):
    """Builds the URL"""
    url = 'https://'
    for i, arg in enumerate(args):
        url += '{}{}'.format(arg.replace('http://', '').replace('https://', '') \
                     .rstrip('/'), '/' if i < len(args)-1 else '')
    return url

def retrieve_from_api(url, apikey=None):
    """Retrieves data from the API"""
    headers = {'Accept-Encoding':'gzip'}
    backoff = 0.1 # ms
    while True:
        response = requests.get(url, auth=HTTPBasicAuth(apikey, ''), headers=headers)
        totalcount = 0
        if 'X-Total-Count' in response.headers.keys():
            totalcount = int(response.headers['X-Total-Count'])
        if response.status_code == 200:
            return totalcount, response.json()
        if response.status_code == 404:
            return None
        if response.status_code == 408:
            sleep(backoff)
            backoff *= 2
        else:
            raise Exception("Cannot retrieve {}: status code: {}".format(url, response.status_code))

def retrieve_page(api_url, object_url, skip=0, top=PAGESIZE, apikey=None, datestr=''):
    """Retrieves a page from the API"""
    # TODO: Rename to retrieve_page_from_api
    if datestr == '':
        url = '{}?skip={}&top={}&count_total=True'.format(build_url(api_url, object_url), skip, top)
    else:
        url = '{}/Search?updated_after_utc={}&skip={}&top={}&count_total=True'\
              .format(build_url(api_url, object_url), datestr, skip, top)
    # TODO: Remove duplication from code above
    return retrieve_from_api(url, apikey=apikey)

def retrieve_api_item_by_id(api_url, object_url, uid, apikey=None):
    """Retrieves a single API item by providing the UID"""
    url = '{}/{}'.format(build_url(api_url, object_url), str(uid))
    return retrieve_from_api(url, apikey=apikey)
