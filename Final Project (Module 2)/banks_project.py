from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = "%Y-%h-%d:%H:%M:$S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")

    # Get all tables
    tables = soup.find_all("table")

    # Second table (index 1)
    target_table = tables[1]

    df = pd.DataFrame(columns=table_attribs)

    rows = target_table.find_all("tr")

    for row in rows[1:]:  # Skip header row
        cols = row.find_all("td")

        if len(cols) >= 3:
            bank_name = cols[1].get_text(strip=True)
            market_cap = cols[2].get_text(strip=True)

            data_dict = {
                "Bank Name": bank_name,
                "MC_USD_Billion": market_cap
            }

            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(df):
    '''
    This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame.
    '''

    # Step 1: Read exchange rate CSV
    rate_df = pd.read_csv('exchange_rate.csv')

    # Step 2: Convert to dictionary
    exchange_rate = rate_df.set_index('Currency').to_dict()['Rate']

    # Step 3: Clean USD column
    df["MC_USD_Billion"] = (
        df["MC_USD_Billion"]
        .str.replace(',', '', regex=False)
        .astype(float)
    )

    # Step 4: Extract rates
    gbp_rate = float(exchange_rate['GBP'])
    eur_rate = float(exchange_rate['EUR'])
    inr_rate = float(exchange_rate['INR'])

    # Step 5: Create new columns
    df["MC_GBP_Billion"] = np.round(df["MC_USD_Billion"] * gbp_rate, 2)
    df["MC_EUR_Billion"] = np.round(df["MC_USD_Billion"] * eur_rate, 2)
    df["MC_INR_Billion"] = np.round(df["MC_USD_Billion"] * inr_rate, 2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Bank Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = 'Largest_banks_data.csv'


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = "SELECT [Bank Name] from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)


log_progress('Process Complete.')

sql_connection.close()
