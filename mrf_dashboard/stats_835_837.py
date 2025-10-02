import pandas as pd
import os
import logging
import csv
import json

from dotenv import load_dotenv
from core.query_utils import collapse_vector, query_postgres, query_redshift
from core.excel_utils import special_write_function

dotenv_path = "/Users/blayton/Documents/env/.env"
load_dotenv(dotenv_path)

log_file_path = os.getenv("LOG_PATH") 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path), #write logs to log file
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

json_file_path = os.getenv("JSON_FILE_PATH")

with open(json_file_path, 'r') as file:
    data = json.load(file) 

analysis_config = data["dashboard_analysis"]
stats_835_837 = data["dashboard_analysis"][0]["stats_835_837"]


products_org_id_query = stats_835_837["products_org_id_query"]
contract_model_query = stats_835_837["contract_model_query"]


def customer_store() -> list:
    """Gets schemas starting with customer_, and dumps them in a list"""

    query = stats_835_837["customer_query"]

    df = query_postgres(query,False,cred_prefix="CLAIMS")
    
    customer_list = df['schema_name'].tolist()
    customer_list.sort()

    return customer_list

def data_frame_prep(valid_customers_df, claims_835_df, claims_837_df):
    processed_835_df = pd.merge(valid_customers_df, claims_835_df, left_on="npi", right_on="npi_835")
    processed_837_df = pd.merge(valid_customers_df, claims_837_df, left_on="npi", right_on="npi_837")
    final_processed_df = pd.merge(processed_835_df, processed_837_df, on=['organization_id', 'name', 'npi', 'org_name', 'customer_name'], how='outer', suffixes=('_835', '_837'))

    return final_processed_df

    #final_df = pd.merge(processed_835_df,processed_837_df,on="npi")

def process_df(df, customer_schema, query):
    if 'customer_name' in df.columns:
        df['customer_name'] = df['customer_name'].str.replace('^customer_', '', regex=True)
    else:
        logger.warning(f"Column 'customer_name' not found in the result for {customer_schema}, {query}")
    
    return df

def valid_customers_to_compare():
    ## Pulling out the orgs from tq_organization_products
    orgs_df = query_redshift(products_org_id_query,False)
    #print(orgs_df)
    patterns_to_filter = r'demo|test|tq|sales|example|delete|mrf'
    orgs_df_filtered = orgs_df[~orgs_df['org_name'].str.lower().str.contains(patterns_to_filter, regex=True)]

    ## Pulling out the specific contact modelling providers
    contract_model_df = query_redshift(contract_model_query, False)

    ## Joining the two to have the complete dataframe to use later. This df has all providers for a given customer as individual rows.
    valid_customers_df = pd.merge(contract_model_df, orgs_df_filtered, on='organization_id',how='inner')

    return valid_customers_df

def claims_extract(customer_schema, logger) -> pd.DataFrame :
    """Taps into query_store(), pulls out query to use for each customer_schema in customer_list
    Returns both an 835 and 837 dataframe containing info for each customer"""

    claims_835_df = pd.DataFrame()
    claims_837_df = pd.DataFrame()

        # Get customer claims stats
    try:
        query_835_template = stats_835_837["query_835"]
        if query_835_template:
            query_835 = query_835_template.format(customer_schema=customer_schema)
            if not query_835:
                logger.info(f"No 835 query made for {customer_schema}.")
        elif not query_835_template:
            logger.error("No 835 query found in JSON.")
        
        query_837_template = stats_835_837["query_837"]
        if query_837_template:
            query_837 = query_837_template.format(customer_schema=customer_schema)
            if not query_837:
                logger.info(f"No 837 query made for {customer_schema}.")
        elif not query_837_template:
            logger.error("No 837 query found in JSON.")

        if query_835:
            try:
                claims_835_df = query_postgres(query_835,False, cred_prefix="CLAIMS")
                if claims_835_df is not None and not claims_835_df.empty:
                    claims_835_df = process_df(claims_835_df, customer_schema, query_835)
                else:
                    logger.info(f"No data returned for {customer_schema}, {query_835}.")


            except Exception as e:
                logger.error(f"Error in the querying of database: {e}")
        
        if query_837:
            try:
                claims_837_df = query_postgres(query_837, False, cred_prefix="CLAIMS")
                if claims_837_df is not None and not claims_837_df.empty:
                    claims_837_df = process_df(claims_837_df, customer_schema, query_837)
                else:
                    logger.info(f"No data returned for {customer_schema}, {query_837}.")

            except Exception as e:
                logger.error(f"Error in the querying of database: {e}")
        
    except Exception as e:
        logger.error(f"Error in building {customer_schema}'s queries.")

    return claims_835_df, claims_837_df

            
def customer_df_prep(customer_schema, logger):
    valid_customers_df = valid_customers_to_compare()
    claims_835_df, claims_837_df = claims_extract(customer_schema, logger)
    
    if not claims_837_df.empty and not claims_835_df.empty:
        final_processed_df = data_frame_prep(valid_customers_df, claims_835_df, claims_837_df)
        return final_processed_df
    else:
        logger.info("Customer does not have claims dataframes to process.")
        return pd.DataFrame()  # Return empty DataFrame instead of undefined variable





