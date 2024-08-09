from utils import (connector, upload_file_to_s3, read_file_of_s3)
import pandas as pd

# client, bucket = authenticate_aws(service='s3', bucket='test-d2p-bucket')

upload_file_to_s3(df=, filename=)

df = read_file_of_s3(bucket_name='test-d2p-bucket', filename=)

def execute_sql_query(sql_script):
    cnx,cur = connector(user='root', host='localhost')
    with open(sql_script,"r") as f:
        content = f.read()
    
    df = pd.read_sql(content,cnx,cur)
    return df     

def process():
    sql_script = "fetch_data.sql"
    data = execute_sql_query(sql_script)
    return data
