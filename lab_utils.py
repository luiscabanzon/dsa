#!/usr/bin/env python

import pandas as pd
import luigi
import sqlite3

from lab_params import *


# Returns objects to interact with database
def connect_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    return conn, cur


# Creates empty table in database
def create_table(table_name, table_schema, drop_if_exists=False):
    conn, cur = connect_db()
    cols = ',\n'.join([f'{col_name} {col_type}' for col_name, col_type in table_schema])
    
    if drop_if_exists:
        sql = f'DROP TABLE IF EXISTS {table_name};'
        print(sql)
        print('\n\n')
        cur.execute(sql)

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            {cols}
        );
    """
    print(sql)
    print('\n\n')
    cur.execute(sql)
    conn.commit()
    conn.close()


# Loads data from file to table in database
def load_file_in_table(
    file_path,
    table_name,
    table_schema=None,
    sep='\t',
    skip_header=True,
    file_encoding='utf-8',
    run_create_table=False,
    overwrite_filter=None,
):
    conn, cur = connect_db()
    
    if run_create_table:
        create_table(table_name, table_schema)
    
    if overwrite_filter:
        sql = f'DELETE FROM {table_name} WHERE {overwrite_filter}'
        print(sql)
        cur.execute(sql)
        print('\n\n')

    print('-----------------------\nFile loading: STARTED\n-----------------------\n')
    with open(file_path, 'r', encoding=file_encoding) as f:
        if skip_header:
            next(f)
        cur.copy_from(f, table_name, sep=sep)
        conn.commit()
    conn.close()

    print('-----------------------\nFile loading: FINISHED\n-----------------------\n')


# Runs query on database, returns output as pandas.DataFrame
def query_db(query):
    conn, cur = connect_db()
    output = pd.io.sql.read_sql_query(query, conn)
    conn.close()
    return output

# Runs query, returns nothing
def run_query(query):
    conn, cur = connect_db()
    cur.execute(query)
    conn.commit()
    conn.close()


# Returns a dataframe listing all tables in database
def get_tables_list():
    return query_db("SELECT name FROM sqlite_master WHERE type='table'")


# Returns True if table exists, False otherwise
def table_exists(table_name):
    return table_name in set(get_tables_list()["name"])


# ##########################
# Luigi auxiliary utilities
# ##########################

class TableExists(luigi.Target):
    def __init__(self, table_name):
        super().__init__()
        self.table_name = table_name

    def exists(self):
        return table_exists()


class DataExists(luigi.Target):
    def __init__(self, table_name, where_clause):
        super().__init__()
        self.table_name = table_name
        self.where_clause = where_clause

    def exists(self):
        if TableExists(self.table_name).exists():
            print(f'LOGGING: Table exists: {self.table_name}')
            return query_db(f'SELECT * FROM {self.table_name} WHERE {self.where_clause} LIMIT 1').size > 0
        else:
            return False


# #########################
# Other auxiliary functions
# #########################

def get_indicator_code(indicator):
    indicator_to_code = {
        'covid': 'cli',
        'flu': 'ili',
        'mask': 'mc',
        'contact': 'dc',
        'finance': 'hf',
        'anosmia': 'anos',
        'vaccine_acpt': 'vu',
    }
    return indicator_to_code.get(indicator, indicator)
