import argparse
import yaml
from src.utils import (upload_file_to_s3, 
                       process_task, 
                       upload_to_google_sheet)

args = argparse.ArgumentParser(
    description="Provides some inforamtion on the job to process"
)
args.add_argument(
    "-t", "--task", type=str, required=True,
    help="This will point to a task location into the config.yaml file.\
        Then it will follow the step of this specific task.")
args = args.parse_args()

with open("./config/config.yaml", 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
    
config_export = config[args.task]["export"]
# config_export = config["modeling"]["export"]

if config_export[0]["export"]["host"] == 's3':
    upload_file_to_s3(df=process_task(args.task), 
                      filename=config_export[0]["export"]["filename"],
                      bucket=config_export[0]["export"]["bucket"])
elif config_export[0]["export"]["host"] == 'gsheet':
    upload_to_google_sheet(spreadsheet_id=config_export[0]["export"]["spread_sheet_id"], 
                           df=process_task(args.task),
                           worksheet_name=config_export[0]["export"]["worksheet_name"])
    