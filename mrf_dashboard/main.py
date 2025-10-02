import pandas as pd
import os
import logging
import csv
import json

from dotenv import load_dotenv
from pathlib import Path
from core.query_utils import collapse_vector, query_postgres, query_redshift
from core.excel_utils import special_write_function
from export_s3 import write_to_s3, redshift_table_management, run_aws_sso_login
from stats_835_837 import customer_store, customer_df_prep

dotenv_path = "/Users/blayton/Documents/env/.env"
load_dotenv(dotenv_path)

log_file_path = os.getenv("LOG_PATH")

# Clear any existing handlers
logging.getLogger().handlers.clear()
#print(logging.getLogger().handlers)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Get logger
logger = logging.getLogger(__name__)

run_aws_sso_login()

json_file_path = os.getenv("JSON_FILE_PATH")
redshift_role = os.getenv("REDSHIFT_ROLE")

with open(json_file_path, 'r') as file:
    data = json.load(file)  

s3_bucket = data["s3_bucket"]
parent_directory = data["parent_directory"]
dashboard_components = data["dashboard_components"]
dashboard_analysis = data["dashboard_analysis"]

## query metadata, then upload csv to s3
def global_components(dashboard_components, logger):
    for component in dashboard_components:
        for key, value in component.items():
            print(f"Component: {key}")
            subdirectory_name = value["subdirectory_name"]
            file_name = value["file_name"]
            table_name = value["table_name"]
            query = value["query"]
            staging_table_query = value.get("staging_table_query", None)
            truncate_query = value.get("truncate_query").format(table_name=table_name)

            try:
                # Fetch data from PostgreSQL
                df = query_postgres(query, False, cred_prefix="CLAIMS")

                # Process the dataframe
                if 'customer_database' in df.columns:
                    df['customer_database'] = (
                        df['customer_database']
                        .str.replace('^legacy_customer_', '', regex=True)
                        .str.replace('^customer_', '', regex=True)
                    )
                    df = df.rename(columns={'customer_database': 'customer_name'})
                else:
                    logger.warning(f"Column 'customer_database' not found in the result.")
            except Exception as e:
                logger.error(f"Error fetching or processing data for component '{key}': {e}")
                continue

            # Save to a CSV file
            try:
                file_path = Path.cwd() / file_name
                df.to_csv(file_path, index=False)
            except Exception as e:
                logger.error(f"Error saving file {file_name} for component '{key}': {e}")
                continue

            # Write the CSV to S3, it will overwrite the existing file
            try:
                s3_path = f"{parent_directory}/{subdirectory_name}/{file_name}"
                write_to_s3(file_path, s3_bucket, s3_path)
                os.remove(file_path)
            except Exception as e:  
                logger.error(f"Error uploading {file_name} to S3 for component '{key}': {e}")
                continue

            ## check if tables exist in redshift. if it doesnt, create them. 
            try:
            # Try to create table if it doesn't exist
                redshift_table_management(table_name, staging_table_query, logger)
                logger.info(f"Created new table {table_name}")
            except Exception as e:
                logger.info(f"Table {table_name} already exists, proceeding with refresh: {e}")
            
            try:
                # Truncate existing table
                print(truncate_query)
                query_redshift(truncate_query, True)
                logger.info(f"Truncation successful for {table_name}")

                # Copy new data
                copy_query = value["copy_query"].format(table_name=table_name, role=redshift_role)
                query_redshift(copy_query, True)
                logger.info(f"Refresh of {table_name} successful using new data from {s3_path}")

            except Exception as e:
                logger.error(f"Error during refresh process for {table_name}: {str(e)}")
                logger.error(f"Failed to refresh table using new data from {s3_path}")
                raise  # Re-raise the exception if you want calling code to handle it
        
## deidenified 835_837 customer stats, functions are held in stats_835_837.py
def analysis_compenents(dashboard_analysis, parent_directory, s3_bucket, redshift_role, logger): 
    for analysis in dashboard_analysis: 
        for key, value in analysis.items():
            try:
                print(f"Analysis component: {key}")
                subdirectory_name = value.get("subdirectory_name")
                file_name = value.get("file_name")
                table_name = value.get("table_name")
                staging_table_query = value.get("staging_table_query")
                truncate_query = value.get("truncate_query").format(table_name=table_name)

                if not subdirectory_name or not file_name or not table_name:
                    logger.error(f"Missing required keys in analysis component '{key}'. Skipping.")
                    continue

                customer_list = customer_store()
                for customer_schema in customer_list:
                    df = customer_df_prep(customer_schema, logger)

                    if df is None or df.empty:
                        logger.warning(f"No data for customer schema {customer_schema}. Skipping.")
                        logging.getLogger().handlers[0].flush() 
                        continue

                    try:
                        formatted_file_name = file_name.format(customer_schema=customer_schema)
                        file_path = Path.cwd() / formatted_file_name
                        df.to_csv(file_path, index=False)
                    except Exception as e:
                        logger.error(f"Error saving file {file_name} for component '{key}': {e}")
                        continue

                    # Write the CSV to S3
                    try:
                        s3_path = f"{parent_directory}/{subdirectory_name}/{formatted_file_name}"
                        write_to_s3(file_path, s3_bucket, s3_path)
                    except Exception as e:
                        logger.info(f"Error uploading {file_name} to S3 for component '{key}': {e}")
                    finally:
                        if file_path.exists():
                            os.remove(file_path)
                            continue

                # Check if table exists in Redshift
                try: 
                    redshift_table_management(table_name, staging_table_query, logger)
                except Exception as e:
                    logger.info(f"Table already exists {table_name}.")

                try:
                    # Truncate existing table
                    print(truncate_query)
                    query_redshift(truncate_query, True)
                    logger.info(f"Truncation successful for {table_name}")

                    # Copy new data
                    copy_query = value["copy_query"].format(table_name=table_name, role=redshift_role)
                    query_redshift(copy_query, True)
                    logger.info(f"Refresh of {table_name} successful using new data from {s3_path}")

                except Exception as e:
                    logger.error(f"Error during refresh process for {table_name}: {str(e)}")
                    logger.error(f"Failed to refresh table using new data from {s3_path}")
                    raise  # Re-raise the exception if you want calling code to handle it

            except Exception as e:
                print(f"Error in building analysis component: {e}")



global_components(dashboard_components, logger)
analysis_compenents(dashboard_analysis,parent_directory, s3_bucket, redshift_role, logger)