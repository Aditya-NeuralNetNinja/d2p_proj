# Imports
import os
from typing import Optional, Tuple
from pandas import DataFrame
import pandas as pd
import mysql.connector as mysql
from dotenv import load_dotenv

load_dotenv()

# STEP 1 - Connect to server
def connector(user:str,
              host:str,
              db:Optional[str]=None) -> Tuple[mysql.MySQLConnection, mysql.cursor.MySQLCursor]:
    """
    Connect via python to mysql server

     Args:
        user_name (str): person operating
        host_name (str): usually localhost
        db_name (Optional[str], optional): Name of database. Defaults to None.

    Returns:
        Tuple[str,str]: Returns connection, cursor

    """
    cnx = mysql.connect(user=user,
                        password=os.getenv('MySQL_Server_Password'),
                        host=host,
                        db=db)
    cur = cnx.cursor()
    return cnx, cur

# STEP 2 - Create db
def build_db(cur:mysql.cursor.MySQLCursor, db:str) -> None:
    """
    Build db

    Args:
        db (str): Input db name
    """
    cur.execute(f'DROP DATABASE IF EXISTS {db};')
    cur.execute(f'CREATE DATABASE {db};')
    cur.execute('SHOW DATABASES')
    databases = cur.fetchall()
    return databases
    
# STEP 3 - Read CSV data
def get_data(file_path:str) -> DataFrame:
    """
    Read CSV data via pandas dataframe

    Args:
        file_path (str): relative file path of input csv

    Returns:
        DataFrame: resultant dataframe
    """
    df = pd.read_csv(file_path)
    df.drop(['Unnamed:0'],axis=1,inplace=True,errors='ignore')
    return df

# STEP 4 - Build Schema

def build_schema(df:DataFrame) -> Tuple[str, str]:
    """
    Convert python style datatypes into sql format datatypes of each column in dataframe
    
    Args:
        df (pd.DataFrame): Input dataframe
    Returns:
        Tuple[str, str]: Returns result, placeholders
    """
    column_dtypes = []
    for col_name in df.columns:
        print(col_name, df[col_name].dtype)
        col_name = col_name.replace('.', '_')  # tackle edge case
        if df[col_name].dtype == 'object':
            data_type = 'VARCHAR(255)'
        elif df[col_name].dtype == 'float64':
            data_type = 'FLOAT'
        elif df[col_name].dtype == 'int64':
            data_type = 'INT'
        else:
            data_type = 'VARCHAR(255)'  # Default type
        column_dtypes.append(f"`{col_name}` {data_type}")
    result = f"({', '.join(column_dtypes)})"
    placeholders = ', '.join(['%s'] * len(df.columns))
    
    return result, placeholders

# STEP 5 - Build Table
def build_table(cur:mysql.cursor.MySQLCursor, db: str, table: str, schema: str) -> None:
    """
    Create Schema with columns
    Args:
        cur (mysql.cursor.MySQLCursor): MySQL cursor
        db (str): Database name
        table (str): Schema name
        schema (str): Table schema
    """
    cur.execute(f'USE {db};')
    cur.execute(f'DROP TABLE IF EXISTS {table};')
    cur.execute(f'CREATE TABLE {table} {schema};')

# STEP 6 - Insert Data into Table
def ingest_data(cur:mysql.cursor.MySQLCursor, cnx:mysql.MySQLConnection, df:DataFrame, table:str, placeholders:str) -> None:
    """
    Insert Data into Table

    Args:
        cur (str):  MySQL cursor
        cnx (str): MySQL connection
        df (DataFrame): Input pandas dataframe
        placeholders (str): Formatted placeholders
    """
    total=0
    for _,row in df.iterrows():
        sql = f"INSERT INTO {table} VALUES({placeholders})"
        val = tuple(row)
        cur.execute(sql, val)
        if cur.rowcount==1:
            total+=1
    cnx.commit()
    return total