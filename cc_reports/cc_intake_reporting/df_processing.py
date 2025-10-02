import pandas as pd
import logging
import json

from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from core.query_utils import collapse_vector, query_postgres


def user_df(query) -> pd.DataFrame:
    """Generates a users df for reuse in compiling results for different reports. 
    Notably this includes tq staff emails. This is because we populate some of a customer's contracts 
    when we first migrate them and we need to retain the TQ staff member that created the contract in the contract reporting."""
    
    result = query_postgres(query, False, "CC_USERS")

    ## populating the full name based on email addresses
    ## this only works because of how CommonSpirit formats emails, when scaling, a different solution will be needed.
    result['local_part'] = result['email'].str.split('@').str[0]
    result['local_part'] = result['local_part'].str.replace(r'\d+', '', regex=True)
    result['full_name'] = result['local_part'].str.split('.').apply(lambda x: ' '.join([part.capitalize() for part in x]))

    result = result.drop('local_part', axis = 1)

    ## handling logic for users that have not logged in for the first time.
    result['first_login'] = result['last_login'].notna()
    return result
  
# this function could be consolidated with report_df_creation?
def org_group_description(query) -> pd.DataFrame:
    """Creates an org group description df"""

    result = query_postgres(query, False, "CC_DATA")

    return result

## user report
def user_report_df(org_group_df: pd.DataFrame, users_for_reports_df: pd.DataFrame) -> pd.DataFrame:
    """Generates the user report df, executes user_df() and org_group_description() 
    this is used to prepare the user report for the CC customer."""
    ## drops TQ users
    users_for_reports_df = users_for_reports_df[~users_for_reports_df['email'].str.contains('turquoise.health')]

    merged_df = pd.merge(users_for_reports_df, org_group_df, on='organization_group_id',how='inner')
    merged_df = merged_df.drop('id', axis = 1)
    merged_df = merged_df.drop('organization_group_id', axis = 1)

    return merged_df

def report_df_processing(user_df: pd.DataFrame, report_query) -> pd.DataFrame:
    """This takes the user df and creates 2 dictionaries. 
    user_mapping dict = {user_id: name}
    user_email_mapping dict = {user_id: email}
    
    The values from the dictionaries are used to map against the string_agg(user_id) columns in the report dataframes."""

    user_mapping = dict(zip(user_df['id'], user_df['full_name']))
    user_email_mapping = dict(zip(user_df['id'], user_df['email']))
    
    # Function to replace user IDs with full names
    def replace_ids_with_names(ids, mapping):
        """Goes through each user_id in the string_agg column of user_ids and replaces the id with the user's full name"""
        if not ids:  # Handle None or empty values
            return ''
        try:
            # Split the string into a list of IDs
            id_list = map(int, ids.split(','))
            # Replace each ID with the corresponding name
            name_list = [mapping.get(user_id, f"Unknown ID: {user_id}") for user_id in id_list]
            # Join the names back into a single string
            return ', '.join(name_list)
        except Exception as e:
            logging.error(f"Error processing IDs: {ids}. Error: {e}")
            return ''

    def replace_ids_with_emails(ids, mapping):
        """Goes through each user_id in the string_agg column of user_ids and replaces the id with the user's email"""
        if not ids:  # Handle None or empty values
            return ''
        try:
            # Split the string into a list of IDs
            id_list = map(int, ids.split(','))
            # Replace each ID with the corresponding name
            email_list = [mapping.get(user_id, f"Unknown ID: {user_id}") for user_id in id_list]
            # Join the names back into a single string
            return ', '.join(email_list)
        except Exception as e:
            logging.error(f"Error processing IDs: {ids}. Error: {e}")
            return ''
    
    # Generate the report DataFrame
    report_df = query_postgres(report_query, False, "CC_DATA")

    # Replace unwanted characters or whitespace in the DataFrame
    report_df = report_df.replace({r'\n': ''}, regex=True)

    # Apply name function to 'created_by_id' column
    if 'created_by_id' in report_df.columns:
        report_df['creator_name'] = report_df['created_by_id'].map(user_mapping)

        report_df['creator_email'] = report_df['created_by_id'].map(user_email_mapping)
        
    if 'last_updated_by_id' in report_df.columns:
        report_df['last_updated_by_name'] = report_df['last_updated_by_id'].map(user_mapping)

        report_df['last_updated_by_email'] = report_df['last_updated_by_id'].map(user_email_mapping)

    if 'contract_owner' in report_df.columns:
        # Apply the function to the contract_owner column
        report_df['contract_owner_names'] = report_df['contract_owner'].apply(
            lambda ids: replace_ids_with_names(str(ids), user_mapping) if pd.notna(ids) else ''
        )

        # Process the emails, create an email column first
        report_df['contract_owner_emails'] = report_df['contract_owner'].apply(
            lambda ids: replace_ids_with_emails(str(ids), user_email_mapping) if pd.notna(ids) else ''
        )

    report_df.drop(columns=['contract_owner', 'created_by_id', 'last_updated_by_id'], errors='ignore', inplace=True)
        
    # Print the resulting DataFrame (optional for debugging)
    print(report_df)

    # Return the processed DataFrame
    return report_df

def datatype_conversion(df) -> pd.DataFrame:
    ## forcing external_id formatting
    specific_cols = ['doc_external_id', 'contract_external_id']
    for col in specific_cols:
        if col in df.columns:
            try:
                df[col] = df[col].apply(lambda x: f"{x}" if pd.notnull(x) else '')
            except ValueError:
                logging.info(f"Could not convert column {col} to float")


    # Remove timezone information from datetime columns
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
        df[col] = df[col].dt.tz_localize(None)
    
    return df