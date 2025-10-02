import trino
import psycopg2
import psycopg
import pandas as pd
import os
import logging
import warnings
import threading

from typing import List
from time import perf_counter
from dotenv import load_dotenv


# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Helper Functions
def collapse_vector(vector_to_collapse: List[str]) -> str:
    """
    Convert a list of strings into a single string formatted for SQL queries.
    """
    string_value = "', '".join(vector_to_collapse)
    return f"'{string_value}'"


def query_end(query_name: str, start_time: float) -> None:
    """
    Log the start and end times of a query and its execution duration.
    """
    end_time = perf_counter()
    logger.info("Success!")
    logger.info(f"{query_name} started @ {start_time}, Ended @ {end_time}")
    logger.info(f"Duration: {end_time - start_time} seconds")


def execute_sql(conn: trino.dbapi.Connection, sql: str) -> None:
    """
    Execute a SQL statement (non-SELECT) using a Trino connection.
    """
    with warnings.catch_warnings():
        try:
            warnings.simplefilter("ignore")
            logger.info(f"Running statement: {sql}")
            cursor = conn.cursor()
            cursor.execute(sql)
        except trino.exceptions.HttpError as e:
            if "io.trino.NotInTransactionException" in str(e):
                logger.info(
                    "Trino raised a NotInTransactionException, likely indicating the table was created."
                )
            else:
                raise e

def pd_execute_sql(conn: trino.dbapi.Connection, sql: str) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a Pandas DataFrame.
    """
    try: 
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            logger.info(f"Running query for DataFrame: {sql}")
            cursor = conn.cursor()
            cursor.execute(sql)
            records = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            return pd.DataFrame(records, columns=column_names)
        
    except Exception as e: 
        logger.warning(f"Error finding customer data: {e}")
            


def query_trino(schema: str, catalog: str, query: str, db_change: bool) -> pd.DataFrame:
    """
    Run a query on a Trino database using a direct connection.
    """
    load_dotenv()
    host = os.getenv("TRINO_HOST")
    port = int(os.getenv("TRINO_PORT", 8080))  # Default port to 8080 if not set
    user = os.getenv("TRINO_USERNAME")
    password = os.getenv("TRINO_PASSWORD")
    catalog = os.getenv("TRINO_CATALOG")

    start_time = perf_counter()

    # Establish connection
    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme="https" if password else "http",
        auth=trino.auth.BasicAuthentication(user, password) if password else None,
    )

    if db_change:
        execute_sql(conn, query)
    else:
        result_df = pd_execute_sql(conn, query)
        query_end(query, start_time)

        return result_df

    query_end(query, start_time)


def query_redshift(query, db_change, columns = None):
    load_dotenv()

    start_time = perf_counter()

    host = os.getenv("REDSHIFT_HOST")
    port = int(os.getenv("REDSHIFT_PORT", 8080))  # Default port to 8080 if not set
    user = os.getenv("REDSHIFT_USERNAME")
    password = os.getenv("REDSHIFT_PASSWORD")
    catalog = os.getenv("REDSHIFT_DATABASE")
        
    try:
        conn = psycopg2.connect(
            dbname=catalog,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        if not db_change:
            cursor.execute(query)
            query_result = cursor.fetchall()

            if columns:
                query_result = pd.DataFrame(query_result, columns=columns)

            else:
                columns = [desc[0] for desc in cursor.description]
                query_result = pd.DataFrame(query_result, columns=columns)
            
            return query_result
            
        else: 
            cursor.execute(query)
            conn.commit()
        
        query_end(' ', start_time)

    except Exception as e:
        logger.warning(f"Error executing query: {e}")
        raise
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'cursor' in locals():
            conn.close()


def pd_execute_psql(conn: psycopg2.extensions.connection, sql: str, params=None) -> pd.DataFrame:
    """
    Execute a PostgreSQL query and return the result as a pandas DataFrame.
    If the query takes longer than 10 minutes, the cursor is closed.
    Includes error handling for missing results, empty parameters, and timeout.
    """
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            logger.info(f"Running query for DataFrame: {sql}")
            cursor = conn.cursor()

            # Function to execute the query
            def execute_query():
                try:
                    if params is None:
                        logger.info("Executing query without parameters.")
                        cursor.execute(sql)
                    elif isinstance(params, list) and len(params) == 0:
                        logger.error("Params list is empty. Cannot execute query.")
                        raise ValueError("Params list cannot be empty")
                    else:
                        logger.info(f"Executing query with params: {params}")
                        cursor.execute(sql, params)
                except psycopg2.Error as e:
                    logger.error(f"Database error: {e}")
                    raise

            # Start the query execution in a thread
            query_thread = threading.Thread(target=execute_query)
            query_thread.start()
            query_thread.join(timeout=600)  # Wait for up to 10 minutes

            if query_thread.is_alive():
                logger.warning("Query took too long to complete. Closing cursor.")
                cursor.close()
                raise TimeoutError("Query execution exceeded 10 minutes and was terminated.")

            # Fetch results if the query completed within the timeout
            records = cursor.fetchall()
            if not records:  # Handle no results case
                logger.warning("Query executed successfully but returned no results.")
                return pd.DataFrame()

            column_names = [desc[0] for desc in cursor.description]
            return pd.DataFrame(records, columns=column_names)

    except psycopg2.Error as db_error:
        logger.error(f"Database error occurred: {db_error}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
    finally:
        try:
            cursor.close()
        except Exception as e:
            logger.warning(f"Error closing cursor: {e}")


def query_postgres(query: str, db_change: bool, cred_prefix: str, params = None) -> pd.DataFrame:
    """
    Run on a postgresql database using direct connection
    """
    load_dotenv()
    
    host = os.getenv(f"{cred_prefix}_HOST")
    port = int(os.getenv(f"{cred_prefix}_PORT", 8080))  # Default port to 8080 if not set
    user = os.getenv(f"{cred_prefix}_USERNAME")
    password = os.getenv(f"{cred_prefix}_PASSWORD")
    catalog = os.getenv(f"{cred_prefix}_DATABASE")
  
    start_time = perf_counter()

    conn = psycopg2.connect(
        dbname=catalog,
        user=user,
        password=password,
        host=host,
        port=port
    )

    if db_change:
        print("Cannot write to db at this time")

    else:
        if not params: 
            result_df = pd_execute_psql(conn, query)

        elif params:     
            result_df = pd_execute_psql(conn, query, params)
            
        query_end(query, start_time)

        if isinstance(result_df.columns, pd.MultiIndex):
            result_df.columns = [" | ".join(map(str, col)) for col in result_df.columns]

        if isinstance(result_df.index, pd.MultiIndex):
            result_df = result_df.reset_index()

        # Flatten lists in DataFrame
        for col in result_df.columns:
            if result_df[col].apply(lambda x: isinstance(x, list)).any():
                result_df[col] = result_df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        
        return result_df
    
    query_end(query, start_time)



