import os
import pandas as pd
import mysql.connector as mysql
from dotenv import load_dotenv
from typing import Optional, Tuple, DataFrame

load_dotenv()

# STEP1 - connect to server
def connector(user:str,
              host:str,
              db:Optional[str]=None) -> Tuple[str,str]:
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
                        password=os.getenv('PASSWORD'),
                        host=host,
                        db=db)
    cur = cnx.cursor()
    return cnx, cur

cnx,cur = connector(user='root', host='localhost')

# STEP 2 - Create db
def build_db(db:str) -> None:
    """Build db

    Args:
        db (str): Input db name
    """
    cur.execute(f'DROP DATABASE IF EXISTS {db};')
    cur.execute(f'CREATE DATABASE {db};')
    
build_db(db='sales')

# STEP 3 - Read CSV data
def get_data(file_path:str) -> DataFrame:
    df = pd.read_csv(file_path)
    return df

df = get_data(file_path='/Users/adi/Desktop/proj/supermarket_sales.csv')
df.head()

# STEP 4 - Build Schema/Table --- how to connect table to df???
def build_table(db:str, table:str, col1:str, col2:str) -> None:
    """Create Schema with columns

    Args:
        db (str): Database name
        table (str): Schema name
        col1 (str): Name of column 1
        datatype1 (str): Column 1 datatype
        col2 (str): Name of column 2
        datatype2 (str): Column 2 datatype
    """
    cur.execute(f'USE {db};')
    cur.execute(f'DROP TABLE IF EXISTS {table};')
    cur.execute(f'CREATE TABLE {table}({col1} {df[col1].dtype}, {df[col2].dtype});')

build_table(db='d2p1',
            table='tutorial',
            col1='id',
            datatype1='int',
            col2='course_name',
            datatype2='varchar(20)')