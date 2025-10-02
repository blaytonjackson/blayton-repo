import boto3
import os
import json
import logging
import subprocess

from botocore.exceptions import NoCredentialsError, ClientError, PartialCredentialsError
from core.query_utils import query_redshift
from dotenv import load_dotenv

dotenv_path = "/Users/blayton/Documents/env/.env"
load_dotenv(dotenv_path)

aws_cli = os.getenv("AWS_CLI")
aws_sso = os.getenv("AWS_SSO")

def run_aws_sso_login():
    try:
        subprocess.run([aws_cli], check = True)
        print("AWS SSO login successful.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during SSO login: {e}")

def connect_s3():
    # Initialize the Boto3 session using the SSO profile
    session = boto3.Session(profile_name=aws_sso)

    # Force the credentials to load
    credentials = session.get_credentials()

    # Ensure that credentials are valid
    if credentials is None or credentials.get_frozen_credentials() is None:
        print("No valid credentials found. Please authenticate via AWS SSO.")

    else:
        # Create an S3 client with the credentials
        s3 = session.client('s3')

    return s3


def write_to_s3(file_name, bucket_name, s3_key):
    """Writes csv to bucket, s3_key describes directory path."""
    s3 = connect_s3()

    #Check if file is already in S3
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        print(f"File '{s3_key}' already exists in bucket '{bucket_name}'. It will be overwritten.")

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"File '{s3_key}' does not exist in bucket '{bucket_name}'. Uploading new file.")
        else:
            print(f"Error occurred while checking for file: {e}")
            return

    # Upload the file to S3
    try:
        s3.upload_file(file_name, bucket_name, s3_key)
        print(f"File {file_name} uploaded successfully to {bucket_name}/{s3_key}")
        
    except NoCredentialsError:
        print("Error: Unable to locate valid credentials.")
    except Exception as e:
        print(f"Error uploading file: {e}")


def delete_file_from_s3(bucket_name, file_key):
    """
    Deletes a file from the specified S3 bucket.

    :param bucket_name: Name of the S3 bucket.
    :param file_key: Key (path) of the file in the bucket.
    """
    try:
        # Initialize the S3 client
        s3_client = boto3.client('s3')

        # Delete the file
        response = s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"File '{file_key}' successfully deleted from bucket '{bucket_name}'.")
        return response

    except NoCredentialsError:
        print("No AWS credentials found. Ensure AWS credentials are configured.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials provided.")
    except Exception as e:
        print(f"An error occurred: {e}")


def redshift_table_management(table_name, staging_table_query, logger):
    """
    Checks if a table exists in Redshift. If it exists, the function does nothing.
    If it does not exist, it creates the table using the provided query and populates it from S3.
    """
    try:
        # Check if the table exists
        exists_query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = '{table_name}'
        );
        """
        result = query_redshift(exists_query, False)  # Assuming this returns a DataFrame or similar
        
        # If the table exists, drop it
        if result.iloc[0][0]:  # Access the first row, first column for the EXISTS result
        
            drop_query = f"""
            DROP TABLE datahouse.public.{table_name}"""

            query_redshift(drop_query, True)
            logger.info(f"Table '{table_name}' exists. Dropping table.")

        # Create the table again
        try:
            logger.info(f"Table '{table_name}' does not exist. Creating table.")
            query_redshift(staging_table_query, True)
            logger.info(f"Table '{table_name}' was successfully created!")

        except Exception as e:
            logger.error(f"Unable to create {table_name}: {e}")

    except Exception as e:
        logger.error(f"Error managing table '{table_name}': {e}")




        