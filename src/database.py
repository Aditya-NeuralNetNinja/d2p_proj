# Imports
import argparse
import os
from pathlib import Path

import yaml

from utils import (build_db, build_schema, build_table, connector, get_data,
                   ingest_data)

# Script description and documentation reference
parser = argparse.ArgumentParser(
    prog= 'Database file',
    description= 'Connect to MySQL server, creates db if non-existent, build schema & table, thereby feed data to table',
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

parser.add_argument('-t','--task_name',
                    type=str,
                    help='Provide task name specificed in yaml file')

args = parser.parse_args()

# load tasks from config --------------------------------------------------------------
with open("./config/config.yaml", 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# STEP 1 - Connect to Mysql server
cnx,cur = connector()

# STEP 2 - Create/Utilize db
if args.db_exists:   
    db = build_db(cur=cur, db=args.db_name)
    print(db)

else:
    # define variables
    # config_import = config[args.task_id]["import"]
    config_import = config["upload-to-database"]["import"]
    for i in range(len(config_import)):
        data = Path(config_import[i]["import"]["dirpath"],
                    config_import[i]["import"]["prefix_filename"] + '.' +
                    config_import[i]["import"]["file_extension"])
        table_name = os.path.basename(data).split('.')[0]
        
        # STEP 3 - Read CSV data
        df = get_data(file_path=data)

        # STEP 4 - Build Schema
        schema, placeholders = build_schema(df=df)

        # STEP 5 - Build Table
        build_table(cur=cur, db=args.db_name, table=table_name, schema=schema)

        # STEP 6 - Insert Data into Table
        counts = ingest_data(cur=cur, cnx=cnx, df=df, table=table_name, placeholders=placeholders)
        print(f'{counts} rows inserted')
        
# STEP 7 - Close cursor, connection
cur.close()
cnx.close() 