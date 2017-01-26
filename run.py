#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '17/05/2016'.

http://stackoverflow.com/questions/20286032/how-to-use-remote-server-sqlite-database-using-python

"""

import os
import sys
import getopt
import sqlite3
import json
import timestring
import calendar
import datetime
from ConfigParser import ConfigParser
from lib.gbif import gbif_downloads
from sortedcontainers import SortedDict
from collections import OrderedDict
from texttable import Texttable, get_color_string, bcolors

help_message = '''
    report.py -q <quarter> -y <year>
'''

COLLECTION_RESOURCES = [
    'bb909597-dedf-427d-8c04-4c02b3a24db3',
    '05ff2255-c38a-40c9-b657-4ccb55ab2feb'
]

# Financial year quarter to months
quarter_months = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9],
    4: [10, 11, 12]
}

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def report_add_entry(report, year, month, key, value):

    # Ensure year exists
    try:
        report[year]
    except KeyError:
        report[year] = SortedDict()

    # Ensure month exists, and set dict if not
    try:
        report[year][month]
    except KeyError:
        report[year][month] = {}

    # If key exists, increment by value; otherwise initialise to value
    try:
        report[year][month][key]
    except KeyError:
        report[year][month][key] = value
    else:
        report[year][month][key] += value

def process_row(report, year, quarter, ts, resource_id, count):

    # If we have year/quarter set, skip if data row doesn't match
    if year and ts.year != year:
        return

    if quarter and ts.month not in quarter_months[quarter]:
        return

    # If resource id is one of the collection resources (index lots or speciemns)
    # download stats will be added as collection; otherwise use other
    if resource_id in COLLECTION_RESOURCES:
        key_prefix = 'collection'
    else:
        key_prefix = 'other'

    report_add_entry(report, ts.year, ts.month, '%s_download_events' % key_prefix, 1)

    # If we have a count, add record downloads
    if count:
        report_add_entry(report, ts.year, ts.month, '%s_records' % key_prefix, count)


def generate_report(year=None, quarter=None):

    dir = os.path.dirname(__file__)
    config = ConfigParser()
    config.read(os.path.join(dir, 'config.cfg'))

    report = SortedDict()
    # gbif = gbif_downloads()

    # for date_str, downloads in gbif.items():

    #     m, y = map(int, date_str.split('-'))

    #     if year and y != year:
    #         continue

    #     if quarter and m not in quarter_months[quarter]:
    #         continue

    #     report_add_entry(report, y, m, 'gbif_records', downloads['records'])
    #     report_add_entry(report, y, m, 'gbif_download_events', downloads['download_events'])


    last_timestamp = 0

    # Load the legacy data
    # This has been derived from redis/celery task queue
    # Which is how we did things before storing the download count in the
    # ckanpackager stats.db
    with open(os.path.join(dir, 'src', 'legacy.json')) as data_file:
        data = json.load(data_file)

        for row in data:
            ts = timestring.Date(row['date'])

            # We want to know what the last timestamp is
            if ts.to_unixtime() > last_timestamp:
                last_timestamp = ts.to_unixtime()

            process_row(report, year, quarter, ts, row.get('resource_id'), row.get('count', None))

    db = config.get('sqlite', 'db')

    if not os.path.isfile(db):
        raise IOError('Stats.db does not exist')

    conn = sqlite3.connect(db)

    # Retrieve all requests received after the last entry in the legacy data
    requests = conn.execute("SELECT * FROM requests WHERE timestamp > '%s'" % last_timestamp)

    # Loop through requests, adding them to the stats
    for request in requests:
        resource_id = request[2]
        ts = datetime.datetime.fromtimestamp(request[3])
        count = int(request[4]) if request[4] else None
        process_row(report, year, quarter, ts, resource_id, count)


    header = OrderedDict([
        ('collection_records', 'Collection records'),
        ('other_records', 'Other records'),
        ('gbif_records', 'GBIF records'),
        ('collection_download_events', 'Collection download events'),
        ('other_download_events', 'Other download events'),
        ('gbif_download_events', 'GBIF download events'),
    ])

    table = Texttable()
    table.set_deco(Texttable.HEADER)
    rows = []

    totals = OrderedDict([(k, 0) for k in header.keys()])

    for year, months in report.items():
        if len(rows) == 0:
            rows.append(['Month'] + header.values())
        for month, items in months.items():
            row = [get_color_string(bcolors.GREEN, '%s %s' % (calendar.month_abbr[month], str(year)[2:4]))]

            for key in header.keys():
                row.append(str(items.get(key, '')))

                # Update totals
                totals[key] += items.get(key, 0)

            rows.append(row)
    rows.append([get_color_string(bcolors.YELLOW, str(t)) for t in ['Totals'] + totals.values()])

    table.add_rows(rows)
    print(table.draw())


def main(argv=None):
    report_params = {}

    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hq:y:", ["help", "quarter=", "year="])
        except getopt.error, msg:
            raise Usage(msg)

        # option processing
        for option, value in opts:
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-q", "--quarter"):
                report_params['quarter'] = int(value)
                # Validate this is a valid quarter
                if report_params['quarter'] not in quarter_months:
                    raise Usage('Please select a quarter: {}'.format(','.join(map(str, quarter_months.keys()))))
            if option in ("-y", "--year"):

                #  Allow '16 - not just 2016
                if len(value) == 2:
                    value = '20' + value

                report_params['year'] = int(value)

        if 'quarter' in report_params and 'year' not in report_params:
            raise Usage('Please enter the year (-y --year) for this quarterly report')

        generate_report(**report_params)

    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())
