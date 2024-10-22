import requests
from bs4 import BeautifulSoup
import  pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# the website = https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks
# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            ancher_data = col[1].find_all('a')[1]
            if ancher_data is not None:
                data_dict = {
                "Name" : ancher_data.contents[0],
                "MC_USD_Billion" : col[2].contents[0]
                 }
                df1 = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df, df1], ignore_index = True)
        

    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    USD_list = list(df['MC_USD_Billion'])
    USD_list = [float(''.join(x.split('\n'))) for x in USD_list]
    # Update the 'MC_USD_Billion' column in the DataFrame with the converted values
    df['MC_USD_Billion'] = USD_list
    return df

def transform(df, csv_path):
    exchange_rate= pd.read_csv(csv_path)
    dict = exchange_rate.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * dict['INR'], 2) for x in df['MC_USD_Billion']]


    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''



url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path  = r'{insert_your_path}'
new_csv_path = r'{insert_your_path}'


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete.Initiating Transforation process')

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, new_csv_path)

log_progress('Data saved to csv')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)

log_progress('Data saved to database')

query_statement_one = f"SELECT * FROM {table_name}"
query_statement_two = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query_statement_three = f"SELECT Name from {table_name} LIMIT 5"

run_query(query_statement_one, sql_connection)
run_query(query_statement_two, sql_connection)
run_query(query_statement_three, sql_connection)
log_progress('Process Complete')

sql_connection.close()

log_progress('Server Connection closed')
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''