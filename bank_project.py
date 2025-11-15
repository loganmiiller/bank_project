from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
table_attribs = ['Name', 'MC_USD_Billions']
csv_p = "./Largest_banks_data.csv" 
db_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./bank_project.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')

def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    df = pd.DataFrame(columns = table_attribs)

    tables = data.find_all('tbody')

    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            if col[1].find('a') is not None:
                data_dict = {"Name": col[1].get_text(strip=True),
                            "MC_USD_Billions": col[2].get_text(strip=True)}
                df1 = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df, df1], ignore_index = True)
    return df

def transform(df, csv_path):
    rates = pd.read_csv(csv_path)
    exchange_rate = dict(zip(rates["Currency"], rates["Rate"]))

    df["MC_USD_Billions"] = df["MC_USD_Billions"].astype(float)

    df['MC_GBP_Billions'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billions']]
    df['MC_EUR_Billions'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billions']]
    df['MC_INR_Billions'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billions']]

    return df

def load_to_csv(df, csv_p):
    df.to_csv(csv_p)

def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists = 'replace', index = False)

def run_queries(query_statement, conn):
    print(query_statement)
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

transform(df, csv_path)

log_progress('Data tranformation complete. Initiating Transformation process')

load_to_csv(df, csv_p)

log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')

load_to_db(df, conn, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f'SELECT * from {table_name}'

run_queries(query_statement, conn)

query_statement = f'SELECT AVG(MC_GBP_Billions) FROM {table_name}'

run_queries(query_statement, conn)

query_statement = f'SELECT Name from {table_name} LIMIT 5'

run_queries(query_statement, conn)

log_progress('Process Complete.')

conn.close()