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

sql = "INSERT INTO tutorial (id,course_name) VALUES (%s, %s);"
value1 = ('1','Python')
value2 = ('2', 'ML')
value3 = ('3', 'Comp Vision')

cur.execute(sql, value1)
cur.execute(sql, value2)
cur.execute(sql, value3)

cnx.commit()
print(cur.rowcount, "Record inserted")

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

"""
mappings = {
    'int64': 'int',
    'float64': 'float',
    'object': 'varchar'
    }
"""

# (timestamp object, temperature float64) : Py
# (timestamp VARCHAR(255), temperature FLOAT) : SQL


column_dtypes = []

for col_name in df.columns:
    print(col_name, df[col_name].dtype)
    col_name = col_name.replace('.','_') #tackle edge case
    if df[col_name].dtype == 'object':
        type = 'VARCHAR(255)'
    elif df[col_name].dtype == 'float64':
        type = 'FLOAT'
    
    column_dtypes.append(f"{col_name} {type}")

result = f"({', '.join(column_dtypes)})"
print(result)

placeholders = ', '.join(len(df.columns)*['%s'])
print(placeholders)

df1=get_data("/Users/adi/Desktop/proj/supermarket_sales.csv")
placeholders = ', '.join(len(df1.columns)*['%s'])
print(placeholders)

total=0
for _,row in df1.iterrows():
    sql = f"INSERT INTO customers VALUES({placeholders})"
    val = tuple(row)
    cur.execute(sql, val)
    if cur.rowcount==1:
        total+=1
cnx.commit()

cur.close()
cnx.close() # type: ignore