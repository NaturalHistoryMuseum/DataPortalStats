#!/usr/bin/env python
# encoding: utf-8
"""

This builds a list of institutions

"""

import csv
import os
import sqlite3
import tldextract
from operator import itemgetter
from fuzzywuzzy import fuzz

from SPARQLWrapper import SPARQLWrapper, JSON, N3
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed

def sparql_query(domain):

    q = """
        PREFIX dbo: <http://dbpedia.org/ontology/>
        SELECT * WHERE
        {{
        ?subject dbo:wikiPageExternalLink <{0}> .
        optional {{
            ?subject rdf:type ?type .
        }}
        optional {{
            ?subject foaf:homepage ?homepage.
            }}
        optional {{
            ?subject geo:lat ?latitude .
            ?subject geo:long ?longitude .
            }}
        optional {{
            ?subject rdfs:label ?label .
            FILTER (lang(?label) = 'en')
            }}
        }}
    """.format(domain)

    records = {}

    sparql = SPARQLWrapper("http://dbpedia.org/sparql")

    try:
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
    except QueryBadFormed:
        print 'Badly formed query: ', domain
    except:
        print '--- Other error ----'
        print q
    else:

        for result in results["results"]["bindings"]:
            uri = result['subject']['value']
            try:
                r = records[uri]
            except KeyError:

                records[uri] = {
                    'uri': uri
                }

                for k in ['homepage', 'label', 'type', 'latitude', 'longitude']:
                    v = result.get(k)
                    if v:
                        v = v['value']
                        if k == 'type':
                            v = [v]

                        records[uri][k] = v

            else:
                # We shouldn't have more than one label or homepage
                # for k in ['homepage', 'label']:
                #     v = result.get(k)
                #     if v and r[k] != v['value']:
                #         print 'DUPLICATE: ', r[k], v['value']
                #         # raise Exception('Duplicate')

                if result['type']['value'] not in r['type']:
                    r['type'].append(result['type']['value'])

    return records

special_case_urls = {
    'tau': ['http://english.tau.ac.il/'],
    'kyoto-u': ['http://www.kyoto-u.ac.jp/en'],
    'griffithuni': ['http://www.griffithuv.com.au/'],
    'rbge': ['http://www.rbge.org.uk'],
    'iwate-u': ['http://www.iwate-u.ac.jp/english/index.html'],
    'ioz': ['http://www.zsl.org'],
    'scbg': ['http://english.scib.ac.cn/']

}

def get_institution(domain):

    records = {}

    # We don't want any subdomains etc.,
    ext = tldextract.extract(domain)

    try:
        urls = special_case_urls[ext.domain]
    except KeyError:
        urls = [
            'http://www.{0}.{1}/'.format(
                ext.domain,
                ext.suffix
            ),
            'http://{0}.{1}/'.format(
                ext.domain,
                ext.suffix
            ),
            'http://www.{0}.{1}'.format(
                ext.domain,
                ext.suffix
            ),
            'http://{0}.{1}'.format(
                ext.domain,
                ext.suffix
            ),
            'https://www.{0}.{1}'.format(
                ext.domain,
                ext.suffix
            ),
            'https://{0}.{1}'.format(
                ext.domain,
                ext.suffix
            )
        ]

        if ext.subdomain and ext.subdomain != 'www':
            urls.append('http://{0}.{1}.{2}'.format(
                ext.subdomain,
                ext.domain,
                ext.suffix
            ))
            urls.append('http://www.{0}.{1}.{2}'.format(
                ext.subdomain,
                ext.domain,
                ext.suffix
            ))

    for url in urls:
        result = sparql_query(url)
        if result:
            records.update(result)
    records = records.values()
    if not records:
        print 'No records: ', domain
    else:
        for record in records:
            record_types = record.get('type')
            homepage = record.get('homepage')
            weight = 0
            if homepage:
                weight += fuzz.ratio(homepage, domain)

            if record_types and (u'http://dbpedia.org/ontology/University' in record_types or u'http://umbel.org/umbel/rc/University' in record_types):
                weight += 100

            record['weight'] = weight

        sorted_records =  sorted(records, key=itemgetter('weight'), reverse=True)

        # print sorted_records

        return sorted_records[0]



def main():

    raise Exception

    academic_domains = set(['edu', 'ac'])

    institutions = {}

    for row in conn.execute('SELECT * FROM requests ORDER BY timestamp'):

        domain = row[1].split('@')[1]
        domain_parts = set(domain.split('.'))

        if domain_parts.intersection(academic_domains):

            try:
                institutions[domain]['count'] += 1
            except KeyError:
                institution = get_institution(domain)

                if institution:
                   institutions[domain] = {
                        'label': institution['label'].encode('utf8'),
                        'longitude': institution.get('longitude', None),
                        'latitude': institution.get('latitude', None),
                        'count': 1
                    }
                else:
                    print 'ERROR NOT FOUND: ', domain

                print len(institutions)

    institutions_list = institutions.values()

    print 'Complete: writing to file'
    with open('/tmp/institutions.csv', 'wb') as outfile:
        fieldnames = institutions_list[0].keys()
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(institutions_list)

    print institutions


if __name__ == '__main__':
    main()

