#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '17/05/2016'.
"""

import re
import sys
import os
import requests
import requests_cache
from datetime import datetime
from bs4 import BeautifulSoup
from ConfigParser import ConfigParser
from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options


OFFSET_INTERVAL = 10  # Num per page, on GBIF known as offset interval

cache_opts = {
             'cache.type': 'file',
             'cache.data_dir': '/tmp/.beaker_cache',
             }

cache = CacheManager(**parse_cache_config_options(cache_opts))

@cache.cache('gbif', expire=14400)
def gbif_downloads():
    """
    Scrape the GBIF site for download events of Museum records

    Cache the results for 4 hours so rerunning with different params doesn't take ages
    :return:
    """

    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.cfg'))
    dataset_uuid = config.get('gbif', 'dataset_uuid')

    downloads = {}

    requests_cache.install_cache('/tmp/.gbif')
    offset = 0

    while True:
        print 'Retrieving page offset %s' % offset

        # Build URL
        url = os.path.join('http://www.gbif.org/dataset', dataset_uuid, 'activity')
        r = requests.get(url,  params={'offset': offset})
        # Get some soup
        soup = BeautifulSoup(r.content)

        records = soup.find_all('div', class_="result")

        if not records:
            break

        for record in records:
            download_dt = record.find("dt", text="Download")
            date_str = download_dt.find_next('dd').contents[2].strip()
            date_str = re.sub(r'(\d)(st|nd|rd|th)', r'\1', date_str)

            date_object = datetime.strptime(date_str, '%d %B %Y')

            key = '%s-%s' % (date_object.month, date_object.year)

            try:
                downloads[key]
            except:
                downloads[key] = {
                    'download_events': 0,
                    'records': 0
                }

            # Lets get the counts
            records_dt = record.find("dt", text="Records")
            num_downloads = int(re.search(r'(\d+)', records_dt.find_next('dd').text).group(1))

            downloads[key]['download_events'] += 1
            downloads[key]['records'] += num_downloads

        # Increment offset by interval
        offset += OFFSET_INTERVAL

    return downloads

if __name__ == '__main__':
    print gbif_downloads()