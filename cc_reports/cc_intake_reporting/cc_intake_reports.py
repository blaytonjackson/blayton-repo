import os
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

from df_processing import user_df, org_group_description, user_report_df, report_df_processing, datatype_conversion
from core.jinja_templating import SQLTemplateLoader
from core.excel_utils import cool_write_function, empty_directory
from core.sftp_management import SFTPManager

# Load .env file
load_dotenv()
logger = logging.getLogger(__name__)

# Base directory is two levels up from this script
base_dir = Path(__file__).resolve().parent

# Load config.json from parent directory
config_path = base_dir / "config.json"
with open(config_path, 'r') as file:
    data = json.load(file)

# Extract configuration values
organization_id = data["organization_id"]
customer_abbreviation = data["customer_abbreviation"]

# Build paths relative to base_dir
output_directory = base_dir / "output"
empty_directory(output_directory)

sql_template_path = base_dir / "sql_templates"

## rendering sql templates using jinja
if __name__ == "__main__":
    sql_loader = SQLTemplateLoader(sql_template_path)
    rendered = sql_loader.render_all_templates(**data)

def report_generation(**kwargs):
    start_time = datetime.now()

    users_df = user_df(kwargs['users_report'])
    users_for_reports_df = users_df.copy()
    del kwargs['users_report']

    # Clears the existing output directory
    empty_directory(output_directory)

    for key, query in kwargs.items():
        if key == 'org_group_description':
            org_df = org_group_description(query)
            key = 'users_report'
            df = user_report_df(org_df, users_for_reports_df)
        else:
            df = report_df_processing(users_df, query)

        df = datatype_conversion(df)
        cool_write_function(customer_abbreviation, output_directory, key, df)
        logger.info(f"{key} component processed.")

    execution_time = datetime.now() - start_time
    logger.info(f"Reports generated in {execution_time.seconds} seconds")

def sftp():
    sftp_creds = data['sftp_creds']
    sftp_host = sftp_creds.get('sftp_host')
    sftp_port = sftp_creds.get('sftp_port')
    remote_output = sftp_creds.get('remote_output')
    parent_directory = sftp_creds.get('parent_directory')

    # Make local path relative to base_dir
    local_path = output_directory

    all_file_paths = []
    current_date = datetime.now().strftime('%Y-%m-%d')
    remote_path = os.path.join(remote_output, parent_directory, current_date)

    logging.info(f"Traversing directory: {local_path}")

    for root, dirs, files in os.walk(local_path):
        for file in files:
            full_path = os.path.join(root, file)
            all_file_paths.append(full_path)

    sftp_username = os.getenv('CSH_SFTP_USERNAME')
    sftp_password = os.getenv('CSH_SFTP_PASSWORD')

    sftp_manager = SFTPManager()
    sftp_manager.connect(hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password)

    sftp_manager.mkdir_p(remote_path)

    for local_file in all_file_paths:
        filename = os.path.basename(local_file)
        file_remote_path = os.path.join(remote_path, filename)
        sftp_manager.upload_file(local_file, file_remote_path)

    sftp_manager.disconnect()

# Run report generation and SFTP upload
report_generation(**rendered)
sftp()

