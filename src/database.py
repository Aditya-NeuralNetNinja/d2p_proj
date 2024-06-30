from utils import connector, build_db, get_data, build_schema, build_table, ingest_data, close_all

# STEP 1 - connect to server
cnx,cur = connector(user='root', host='localhost')

# STEP 2 - Create db
build_db(cur=cur, db='sales')

# STEP 3 - Read CSV data
df = get_data(file_path='data/supermarket_sales.csv')

# STEP 4 - Build Schema
schema, placeholders = build_schema(df)

# STEP 5 - Build Table
build_table(cur=cur, db='sales', table='sales_data', schema=schema)

# STEP 6 - Insert Data into Table
ingest_data(cur=cur, cnx=cnx, df=df, placeholders=placeholders)
    
# STEP 7 - Close cursor, connection
close_all(cur=cur, cnx=cnx)