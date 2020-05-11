# Elasticslurp

This program is a very basic way to identify and investigate open ElasticSearch
servers. It is *not* intended to archive or scrape complete ElasticSearch
databases, only to sample a bit of data from a wide range of indexes that you
can later data-mine to see if they contain interesting fields.

It's implemented in a multi-stage scraping process and all information is
stored in a SQLite database. The final document samples can be exported to
JSON and further investigated with tools like grep, jq, or even just a text
editor.

This app requires Shodan Query API credits, which it uses to find the IP addresses
of open ElasticSearch boxes. You can check whether you have any credits by
logging in to your Shodan account and visiting [this page](https://developer.shodan.io/dashboard).

## Install Instructions

```bash
git clone https://github.com/nemec/elasticslurp.git
cd elasticslurp/
python3 -m venv env  # create virtual environment
source env/bin/activate  # activate virtual environment
pip3 install -r requirements.txt
cp config.py.default config.py
```

Now edit `config.py` to add your Shodan API key (found on
[this page](https://account.shodan.io/)).

## Usage

Follow each step in order. Also, ensure you have activated your virtual
environment, otherwise the packages will be missing.

### Create Database

This database holds data related to one group of search queries. Since SQLite
produces database files with little overhead, you should create a new database
each time you want to sample data.

```bash
python3 main.py create customer.db
```

### Search Shodan

The search command will search for ElasticSearch databases on any port
matching the provided keyword. A summary of results will tell you how many
databases were found and how many new IP addresses were added to the project
database. This command can be repeated with multiple keywords or variations
on a keyword (e.g. `customer`, `customers`) to append all results to the same
database.

```bash
python3 main.py search --database customer.db customer
# Total results for keyword "customer": 176
# New IP addresses added: 177
```

Inside the `IP_SEARCH_RESULT` table in the database you'll find additional
info about the results that were added, including Organization (Tencent,
Amazon, etc.) and Location.

### Scraping Index Information

The third step is to scrape the complete list of Elastic indexes from each
host that was added to the database in the previous step. If a host is not online
(common with Shodan, since they cache results for some time), a warning will be
added to the console output and the host will be ignored when retrieving samples.
This process is parallelized, but may take some time if there are many offline
hosts.

```bash
python3 main.py scrape --database customer.db
# Scraping 1.209.255.255:9200
# Exception connecting to IP 1.209.255.255:9200: ConnectionError(<urllib3.connection.HTTPConnection object at 0x7f4093379160>: Failed to establish a new connection: [Errno 111] Connection refused) caused by: NewConnectionError(<urllib3.connection.HTTPConnection object at 0x7f4093379160>: Failed to establish a new connection: [Errno 111] Connection refused)
# Scraping 101.132.255.255:9200
# Scraped 5 indexes from IP 101.132.255.255:9200
# Scraping 101.201.255.255:9200
# Scraped 7 indexes from IP 101.201.255.255:9200
# 100%|███████████████████████████████████████████| 5/5 [00:02<00:00,  2.44it/s]
```

Index data is stored in the `ES_INDEXES` table and includes info such as
document count for each index and the size of the entire index.

The configuration variable `INDEX_EXCLUSION_LIST_REGEXES` is a list of regular
expressions which, if matched anywhere in the string, will cause the index to
be ignored. Use this to hide common indexes containing useless data (like metrics).

### Sampling Data

The sample step downloads a few documents from each index which you can browse
later to find interesting data. By default, at least 10 documents per index
are sampled, but this can be controlled with the `--count` argument.

```bash
python3 main.py sample --database customer.db
# 100%|█████████████████████████████████████████| 15/15 [00:03<00:00,  4.23it/s]
```

### Displaying Sampled Data

Once you've collected enough data, you can dump all samples from the database
into a JSON file for further analysis (the samples are also available in
the `ES_SAMPLES` table). Samples are dumped to stdout, so you can
pipe it to another program or to a file.

```bash
python3 main.py dump --database customer.db
```
```bash
python3 main.py dump --database customer.db > samples.json
```
```bash
python3 main.py dump --database customer.db | grep 'gmail'
```
```bash
python3 main.py dump --database customer.db | \
    jq '.[].data._source.name' -cr | \  # find the 'name' field inside the document
    grep -v '^null$'  # jq will output null if it can't find the key in the document
```