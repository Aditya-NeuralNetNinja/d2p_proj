# Imports
from utils import (connector, build_db, get_data, 
                   build_schema, build_table, ingest_data)
import argparse
import os

# Script description and documentation reference
parser = argparse.ArgumentParser(
    prog= 'Database file',
    description= 'Connect to mysqlserver, creates db if non-existent, build schema & table, thereby feed data to table',
    epilog= 'https://docs.python.org/3/howto/argparse.html'
)

# Parse script arguments
parser.add_argument('-de','--db_exists',
                    type=bool,
                    default=False,
                    help='Check whether db exists or not. Set to "True" to create new db, lest "False" to use existing db')

parser.add_argument('-dn', '--db_name',
                    type=str,
                    required=True,
                    help='Name of database')

parser.add_argument('-f','--file_path',
                    type=str,
                    help='Location of file in directory')

parser.add_argument('-t','--timestamp',
                    type=str,
                    default='timestamp',
                    help='Column associated with date,time')

args = parser.parse_args()

# STEP 1 - connect to Mysql server
cnx,cur = connector(user='root', host='localhost')

# STEP 2 - Create/Utilize db
if args.db_exists:   
    db = build_db(cur=cur, db=args.db_name)
    print(db)

else:
# STEP 3 - Read CSV data
    df = get_data(file_path=args.file_path)
    table_name = os.path.basename(args.file_path).split('.')[0]

# STEP 4 - Build Schema
    schema, placeholders = build_schema(df=df)

# STEP 5 - Build Table
    build_table(cur=cur, db=args.db_name, table=table_name, schema=schema)

# STEP 6 - Insert Data into Table
    counts = ingest_data(cur=cur, cnx=cnx, df=df, table=table_name, placeholders=placeholders)
    print(f'{counts} rows inserted')
        
# STEP 7 - Close cursor, connection
cur.close()
cnx.close() # type: ignore