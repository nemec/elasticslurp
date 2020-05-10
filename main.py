import sqlite3
from shodan import Shodan, APIError
import pandas as pd
from elasticsearch import Elasticsearch, ElasticsearchException, RequestsHttpConnection
import argparse
import pathlib
import sys
import bitmath
import json
from datetime import datetime

from config import ELASTIC_API_KEY

def search(db_file, keyword, api_key):
    api = Shodan(api_key)
    #query = 'product:elastic port:9200'
    query = 'product:elastic'
    if keyword is not None:
        query += ' ' + keyword
    count_results = api.count(query)
    print(f'Total results for keyword "{keyword}": {count_results["total"]}')
    count = 0

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


def scrape(db_file):
    #ip_addresses = [('47.111.183.77', 9200)]
    ip_addresses = []
    with sqlite3.connect(str(db_file)) as conn:
        cur = conn.cursor()
        try:
            ip_addresses = cur.execute('SELECT IP_ADDRESS, PORT FROM IP_SEARCH_RESULT').fetchall()
        finally:
            cur.close()
    length = len(ip_addresses)
    for idx, (ip, port) in enumerate(ip_addresses):
        print(f'({idx}/{length}) Scraping {ip}:{port}')
        try:
            es = Elasticsearch([{"host": ip, "port": port}])
            indices = es.indices.stats('_all')['indices']
            if not indices:
                print(f'No indexes for IP {ip}')
                continue
            results = []
            for idx, data in indices.items():
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
            with sqlite3.connect(str(db_file)) as conn:
                cur = conn.cursor()
                try:
                    cur.executemany(
                            'INSERT OR REPLACE INTO ES_INDEXES '
                            '(IP_ADDRESS, PORT, "INDEX", UUID, DOCS_COUNT, DOCS_DELETED, STORE_SIZE, STORE_SIZE_BYTES, UPDATED_DATE) '
                            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            results)
                    conn.commit()
                finally:
                    cur.close()
            print(f'Scraped {len(results)} indexes from IP {ip}:{port}')

        except ElasticsearchException as e:
            print(f'Exception connecting to IP {ip}:{port}: {e}')





def sample(db_file:pathlib.Path, dump: bool):
    if dump:
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

    ip_addresses = []
    with sqlite3.connect(str(db_file)) as conn:
        cur = conn.cursor()
        try:
            ip_addresses = cur.execute('SELECT UUID, IP_ADDRESS, PORT, "INDEX" FROM '
                                       'ES_INDEXES WHERE DOCS_COUNT > 0 ORDER BY IP_ADDRESS, PORT').fetchall()
                                       
            print('[', end='')
            first = True
            length = len(ip_addresses)
            last_ip = None
            for idx, (uuid, ip, port, index) in enumerate(ip_addresses):
                print(f'({idx}/{length}) Sampling data from {ip}:{port}, index {index}')
                try:
                    es = Elasticsearch([{"host": ip, "port": port}])
                    for result in es.search(index=index)['hits']['hits']:

                        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        cur.execute(
                                'INSERT OR REPLACE INTO ES_SAMPLES '
                                '(UUID, IP_ADDRESS, PORT, "INDEX", SAMPLE, UPDATED_DATE) '
                                'VALUES (?, ?, ?, ?, ?, ?)',
                                (uuid, ip, port, index, json.dumps(result, indent=2), date))
                        # commit about once per host
                        if last_ip is not None and last_ip != ip:
                            conn.commit()
                        last_ip = ip
                except ElasticsearchException as e:
                    print(f'Exception connecting to IP {ip}:{port}: {e}')
            print(']')
        finally:
            conn.commit()
            cur.close()



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
    sample_p.add_argument('--dump', action='store_true', default=False,
                        help='Dump the results to stdout as a json file')

    create_p = subp.add_parser('create', help='Create an empty sqlite database for storing results')
    create_p.add_argument('database', type=pathlib.Path, default=pathlib.Path('data.db'),
                        nargs='?', help='Database file')

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
        search(args.database, args.keyword, args.api_key or ELASTIC_API_KEY)
    elif args.subparser_name == 'scrape':
        scrape(args.database)
    elif args.subparser_name == 'sample':
        sample(args.database, args.dump)
