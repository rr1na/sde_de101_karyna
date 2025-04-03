# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server



# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases

#alternativa

duckdb.query("INSTALL sqlite_scanner; LOAD sqlite_scanner;")
duckdb.query("ATTACH 'source_database.sqlite' AS sqlite_db")
df = duckdb.query("SELECT * FROM sqlite_db.your_table").df()
df.to_sql('your_table', duckdb.connect('target_database.duckdb'), if_exists='replace', index=False)

# Fetch data from the SQLite Customer table
import sqlite3
sqlite_conn = sqlite3.connect('tpch.db')
customers = sqlite_conn.execute("SELECT * FROM Customer"
).fetchall()

# Insert data into the DuckDB Customer table
import duckdb

duckdb_conn = duckdb.connect('duckdb.db')

insert_query = f"""
INSERT INTO Customer (customer_id, zipcode, city, state_code, datetime_created, datetime_updated)
VALUES (?, ?, ?, ?, ?, ?)
"""

con = duckdb_conn.executemany(insert_query, customers)

# Hint: Look for Commit and close the connections

con.commit()

# Commit tells the DB connection to send the data to the database and commit it, if you don't commit the data will not be inserted

# We should close the connection, as DB connections are expensive
con.close()
sqlite_conn.close()





# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access
import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED)
)
                        
# Download the CSV file from S3

response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
compressed_data = response['Body'].read()

# Decompress the gzip data

import gzip
decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
# Read the CSV file using csv.reader

import csv
import io
csv_reader = csv.reader(io.StringIO(decompressed_data))

data = list(csv_reader) #next() for getting the headers

# Connect to the DuckDB database (assume WeatherData table exists)
import duckdb
duckdb_conn = duckdb.connect('duckdb.db')

duckdb_conn.execute("DESCRIBE WeatherData").fetchall()

insert_query = f"""
INSERT INTO WeatherData (id, date, element, value, m_flag, q_flag, s_flag, obs_time )
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

# Insert data into the DuckDB WeatherData table
con = duckdb_conn.executemany(insert_query, data)
con.commit()
con.close()




# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API

import requests
import json
import duckdb

response = requests.get(url)
data = response.json()["data"]

# Connect to the DuckDB database
duckdb_conn = duckdb.connect("duckdb.db")
# Insert data into the DuckDB Exchanges table
duckdb_conn.execute("DESCRIBE Exchanges").fetchall()

insert_query = f"""
INSERT INTO  Exchanges (id, name, rank, percentTotalVolume, volumeUsd, tradingPairs, socket, exchangeUrl, updated)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py
insert_data = [
    (
        exchange["exchangeId"],
        exchange["name"],
        int(exchange["rank"]),
        (
            float(exchange["percentTotalVolume"])
            if exchange["percentTotalVolume"]
            else None
        ),
        float(exchange["volumeUsd"]) 
        if exchange["volumeUsd"] else None,
        exchange["tradingPairs"],
        exchange["socket"],
        exchange["exchangeUrl"],
        int(exchange["updated"])
    )
    for exchange in data
]

duckdb_conn.executemany(insert_query, insert_data)
duckdb_conn.commit()
duckdb_conn.close()

# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python

import csv
import duckdb

file_path = "data/customers.csv"
with open(file_path, 'r', encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    header = next(csv_reader)
    insert_data = list(csv_reader)

duckdb_conn = duckdb.connect("duckdb.db")
duckdb_conn.execute("DESCRIBE Customer").fetchall()

insert_query = f"""INSERT INTO Customer (customer_id, zipcode, city, state_code, datetime_created, datetime_updated)
VALUES (?,?,?,?,?,?)
"""

duckdb_conn.executemany(insert_query, insert_data)
duckdb_conn.commit()
duckdb_conn.execute("SELECT * FROM Customer LIMIT 10;").fetchall()
duckdb_conn.close()





# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
url = 'https://example.com'
import requests
from bs4 import BeautifulSoup

try:
    response = requests.get(url)
    response.raise_for_status()
    soup  = BeautifulSoup(response.text, 'html.parser')
    
    for link in soup.find_all('a'):
        print(link.get('href'))

except requests.exceptions.RequestException as e:
    print(f"Error fetching URL: {e}")
    exit()


soup.text
