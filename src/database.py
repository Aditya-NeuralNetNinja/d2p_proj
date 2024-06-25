from utils import connector, build_db, get_data, build_table

# STEP 1 - connect to server
cnx,cur = connector(user='root', host='localhost')

# STEP 2 - Create db
build_db(db='sales')

# STEP 3 - Read CSV data
df = get_data(file_path='/Users/adi/Desktop/proj/supermarket_sales.csv')