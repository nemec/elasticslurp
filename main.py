import argparse
import itertools
import json
import pathlib
import re
import sqlite3
import sys
import time
from datetime import datetime

import bitmath
import pandas as pd
from elasticsearch import (Elasticsearch, ElasticsearchException,
                           RequestsHttpConnection)
from shodan import APIError, Shodan
from tqdm.contrib.concurrent import process_map


from config import SHODAN_API_KEY, INDEX_EXCLUSION_LIST_REGEXES



def search(db_file, keyword, api_key):
    api = Shodan(api_key)
    #query = 'product:elastic port:9200'
    query = 'product:elastic'
    if keyword is not None:
        query += ' ' + keyword
    count_results = api.count(query)
    print(f'Total results for keyword "{keyword}": {count_results["total"]}')

    with sqlite3.connect(str(db_file)) as conn:
        cur = conn.cursor()
        try:
            existing_r = cur.execute('SELECT COUNT(*) FROM IP_SEARCH_RESULT').fetchone()
            existing = existing_r[0] if len(existing_r) > 0 else 0

            results = []
            for result in api.search_cursor(query):
                
                ip = result['ip_str']
                port = result['port']
                org = result['org']
                cntry = result['location']['country_code3']
                loc = f"{result['location']['country_name']} ({result['location']['country_code']})"
                lat = result['location']['latitude']
                lon = result['location']['longitude']
                date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                results.append((ip, port, org, cntry, loc, lat, lon, query, date))
            cur.executemany(
                    'INSERT OR REPLACE INTO IP_SEARCH_RESULT '
                    '(IP_ADDRESS, PORT, ORGANIZATION, COUNTRY_CODE, LOCATION, '
                    'LATITUDE, LONGITUDE, ORIGINAL_SEARCH_QUERY, UPDATED_DATE) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    results)
            conn.commit()

            now_r = cur.execute('SELECT COUNT(*) FROM IP_SEARCH_RESULT').fetchone()
            now = now_r[0] if len(now_r) > 0 else 0
            print(f'New IP addresses added: {now - existing}')

        finally:
            conn.commit()
            cur.close()


def _scrape_parallel(args):
    results = []
    try:
        ip, port = args
        print(f'Scraping {ip}:{port}')
        es = Elasticsearch([{"host": ip, "port": port}])
        indices = es.indices.stats('_all')['indices']
        if not indices:
            print(f'No indexes for IP {ip}')
            return []
        for idx, data in indices.items():
            exclude_index = False
            if any(map(lambda rx: re.search(rx, idx),
                       INDEX_EXCLUSION_LIST_REGEXES)):
                continue
            uuid = data.get('uuid') or idx
            docs_count = data['total']['docs']['count']
            docs_deleted = data['total']['docs']['deleted']
            store_size_bytes = data['total']['store']['size_in_bytes']
            bm = bitmath.Byte(store_size_bytes)
            store_size = bm.best_prefix(bitmath.SI).format('{value:.0f} {unit}')
            results.append((
                ip, port, idx, uuid, docs_count, docs_deleted,
                store_size, store_size_bytes,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        print(f'Scraped {len(results)} indexes from IP {ip}:{port}')
    except ElasticsearchException as e:
        print(f'Exception connecting to IP {ip}:{port}: {e}')
    return results

def scrape(db_file):
    ip_addresses = []
    with sqlite3.connect(str(db_file)) as conn:
        cur = conn.cursor()
        try:
            ip_addresses = cur.execute('SELECT IP_ADDRESS, PORT FROM IP_SEARCH_RESULT').fetchall()
    
            indexes = process_map(_scrape_parallel, ip_addresses, max_workers=4)
            flattened = list(itertools.chain(*indexes))
            cur.executemany(
                    'INSERT OR REPLACE INTO ES_INDEXES '
                    '(IP_ADDRESS, PORT, "INDEX", UUID, DOCS_COUNT, DOCS_DELETED, STORE_SIZE, STORE_SIZE_BYTES, UPDATED_DATE) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    flattened)
            conn.commit()
        finally:
            cur.close()


def _sample_parallel(args):
    uuid, ip, port, index, db_file, sample_count = args
    with sqlite3.connect(str(db_file)) as conn:
        cur = conn.cursor()
        try:
            results = []
            es = Elasticsearch([{"host": ip, "port": port}])
            response = es.search(index=index, size=sample_count)
            if not response.get('hits', {}).get('hits', {}):
                return
            for result in response['hits']['hits']:
                date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sample = json.dumps(result, indent=2)
                idnum = result['_id']
                results.append((idnum, uuid, ip, port,
                                index, sample, date))
            tries = 3
            while tries > 0:
                try:
                    cur.executemany(
                            'INSERT OR REPLACE INTO ES_SAMPLES '
                            '(DOCUMENT_ID, UUID, IP_ADDRESS, PORT, "INDEX", SAMPLE, UPDATED_DATE) '
                            'VALUES (?, ?, ?, ?, ?, ?, ?)',
                            results)
                    conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    print(f'Database locked. Retrying {tries} more times')
                tries -= 1
                time.sleep(1)
        except ElasticsearchException as e:
            print(f'Exception connecting to IP {ip}:{port}: {e}')
                

def sample(db_file:pathlib.Path, count: int):
    ip_addresses = []
    with sqlite3.connect(str(db_file)) as conn:
        ip_addresses = conn.execute('SELECT UUID, IP_ADDRESS, PORT, "INDEX" FROM '
                                    'ES_INDEXES WHERE DOCS_COUNT > 0 ORDER BY IP_ADDRESS, PORT').fetchall()
        samples = process_map(_sample_parallel,
            [(*data, db_file, count) for data in ip_addresses], max_workers=4)


def dump_samples(db_file: pathlib.Path):
    with sqlite3.connect(str(db_file)) as conn:
        cur = conn.cursor()
        samples = cur.execute('SELECT "INDEX", IP_ADDRESS, PORT, '
                                    'SAMPLE FROM ES_SAMPLES'
                                    ).fetchall()
        print('[', end='')
        first = True
        for index, ip, port, sample in samples:
            data = {
                'host': ip,
                'port': port,
                'index': index,
                'data': json.loads(sample)
            }
            if not first:
                print(',')
            else:
                first = False
            print(json.dumps(data, indent=2), end='')
        print(']')
    return


def create_database(db_file: pathlib.Path):
    base_folder = pathlib.Path('sql')
    sql_files = [
        base_folder / 'ip_search_result.sql',
        base_folder / 'es_indexes.sql',
        base_folder / 'es_samples.sql'
    ]

    with sqlite3.connect(str(db_file)) as conn:
        cur = conn.cursor()
        try:
            for sql in sql_files:
                with sql.open('r') as f:
                    cur.executescript(f.read())
        finally:
            conn.commit()
            cur.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subp = parser.add_subparsers(dest='subparser_name')

    create_p = subp.add_parser('create', help='Create an empty sqlite database for storing results')
    create_p.add_argument('database', type=pathlib.Path, default=pathlib.Path('data.db'),
                        nargs='?', help='Database file')

    search_p = subp.add_parser('search', help='Search Shodan.io for Elasticsearch databases matching a keyword.')
    search_p.add_argument('--database', type=pathlib.Path, default=pathlib.Path('data.db'),
                        help='Database file')
    search_p.add_argument('--api-key',
                        help='Shodan API key')
    search_p.add_argument('keyword')

    scrape_p = subp.add_parser('scrape', help='Query each Elasticsearch server and retrieve the names of each index')
    scrape_p.add_argument('--database', type=pathlib.Path, default=pathlib.Path('data.db'),
                        help='Database file')
    
    sample_p = subp.add_parser('sample', help='Query each scraped index for a few sample documents to review for further analysis.')
    sample_p.add_argument('--database', type=pathlib.Path, default=pathlib.Path('data.db'),
                        help='Database file')
    sample_p.add_argument('--count', type=int, default=10,
                        help='Number of documents to sample per-index.')

    dump_p = subp.add_parser('dump', help='Dump collected samples as one JSON object')
    dump_p.add_argument('--database', type=pathlib.Path, default=pathlib.Path('data.db'),
                        help='Database file')

    args = parser.parse_args()
    if args.subparser_name == 'create':
        if args.database.exists():
            print(f"Database file '{args.database}' already exists.")
            sys.exit(1)
        create_database(args.database)
        sys.exit(0)

    if not args.database.exists():
        print(f"Database file '{args.database}' does not exist")
        sys.exit(1)

    if args.subparser_name == 'search':
        search(args.database, args.keyword, args.api_key or SHODAN_API_KEY)
    elif args.subparser_name == 'scrape':
        scrape(args.database)
    elif args.subparser_name == 'sample':
        sample(args.database, args.count)
    elif args.subparser_name == 'dump':
        dump_samples(args.database)
