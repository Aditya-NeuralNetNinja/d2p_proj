# Imports
import argparse
import os
from utils import (connector, build_db, get_data, build_schema, build_table, ingest_data)

# Script description and documentation reference
parser = argparse.ArgumentParser(
    prog= 'Database file',
    description= 'Connect to MySQL server, creates db if non-existent, build schema & table, thereby feed data to table',
    epilog= 'https://docs.python.org/3/howto/argparse.html'
)

# Parse script arguments
parser.add_argument('-u','--user',
                    type=str,
                    default='admin',
                    help='Username to connect with MySQL server')

parser.add_argument('-h','--host',
                    type=str,
                    required=True,
                    help='Host name of MySQL server')

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

args = parser.parse_args()

# STEP 1 - Connect to Mysql server
cnx,cur = connector(user=args.user, host=args.host)

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