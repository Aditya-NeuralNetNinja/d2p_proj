import os
import mysql.connector as mysql
from dotenv import load_dotenv
from typing import Optional, Tuple

load_dotenv()

def connector(user_name: str,
              host_name: str, 
              db_name: Optional[str] = None) -> Tuple[str, str]:
    """Connect to mySQL server or database using this function.

    Args:
        user_name (str): person operating
        host_name (str): usually localhost
        db_name (Optional[str], optional): Name of database. Defaults to None.

    Returns:
        Tuple[str,str]: Returns connection, cursor
    """
    
    cnx = mysql.connect(user=user_name,
                        password=os.getenv('PASSWORD'),
                        host=host_name,
                        database=db_name)

    cur = cnx.cursor()
    return cnx, cur

cnx, cur = connector(user_name='root', host_name='localhost')
'''
query = 'CREATE DATABASE d2p;'
cur.execute(query)

query= 'SHOW DATABASES;'
cur.execute(query)
cur.fetchall()
'''    

#STEP 1: Create db
def build_db(db: str) -> None:
    """Build database from scratch

    Args:
        db (str): Database name
    """
    cur.execute(f'DROP DATABASE IF EXISTS {db};')
    cur.execute(f'CREATE DATABASE {db};')

build_db(db='d2p1')

# STEP 2: Create Schema/Table
def build_table(db:str, table:str, col1:str, datatype1: str, col2:str, datatype2:str) -> None:
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
    cur.execute(f'CREATE TABLE {table}({col1} {datatype1}, {col2} {datatype2});')

build_table(db='d2p1',
            table='tutorial',
            col1='id',
            datatype1='int',
            col2='course_name',
            datatype2='varchar(20)')

# STEP 3: Insert data into table
def data_ingest(table:str, id1:int, course1:str, id2:int, course2:str, id3: int, course3:str):
    """Create entries in table

    Args:
        table (str): Table name
        id1 (int): Unique ID no. of 1st course
        course1 (str): Course1 name
        id2 (int): Unique ID no. of 2nd course
        course2 (str): Course2 name
        id3 (int): Unique ID no. of 3rd course
        course3 (str): Course3 name
    """
    print(course1)
    cur.execute(f"INSERT INTO {table} VALUES({id1}, {course1});")
    cur.execute(f"INSERT INTO {table} VALUES({id2}, '{course2}');")
    cur.execute(f"INSERT INTO {table} VALUES({id3}, '{course3}');")
    
data_ingest(table="tutorial",
            id1=1,
            course1="'Python'",
            id2=2,
            course2="ML",
            id3=3,
            course3="Comp Vision")

cnx.commit()

# STEP 4: CSV -> Table Schema (python to sql) -> Insert data
import pandas as pd

def get_data(file_path):
    df=pd.read_csv(file_path)
    df.drop(['Unnamed:0'],axis=1,inplace=True,errors='ignore')
    return df

df= get_data('/Users/adi/Desktop/proj/stock_temp_agg_cleaned_data.csv')
df.info()
df.dtypes
df.columns

'''
mappings = {
    'int64': 'int',
    'float64': 'float',
    'object': 'varchar'
    }
'''



cur.close()
cnx.close() # type: ignore