import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import awswrangler as wr
import base64
import boto3

def snow_connect(snow_credential):
    """Create a connection to Snowflake.

    Parameters:
        snow_credential (dict): Dictionary containing Snowflake credentials and details.
            snow_user (str): Snowflake username.
            snow_password (str): Snowflake password.
            snow_account (str): Snowflake account.
            snow_db (str): Snowflake database name.
            snow_schema (str): Snowflake schema name.
            snow_wh (str): Snowflake warehouse name.
            snow_role (str): Snowflake role.
    Returns:
        engine: SQLAlchemy engine instance.
    """
    # Extract Snowflake credentials from the input dictionary
    snow_user = snow_credential['snow_user']
    snow_account = snow_credential['snow_account']
    snow_db = snow_credential['snow_db']
    snow_schema = snow_credential['snow_schema']
    snow_wh = snow_credential['snow_wh']
    snow_role = snow_credential['snow_role']
    snow_password = snow_credential['snow_password']
    # snow_key_path = snow_credential.get('snow_key_path')


    # Create the Snowflake URL using the extracted credentials
    url = URL(
        user=snow_user, 
        password=snow_password,
        account=snow_account, 
        database=snow_db, 
        schema=snow_schema,
        warehouse=snow_wh, 
        role=snow_role
    )

    # Create the Snowflake engine using the URL
    snow_engine = create_engine(url)

    # Return the engine
    return snow_engine

def snow_extract(snow_credential, query):
    """Extract data from snowflake using only select query

    Parameters:
        snow_credential (dict): Dictionary containing Snowflake credentials and details.
        query (str): SQL query to extract data from Snowflake.
    Returns:
        df: Extracted data as Pandas DataFrame.
    """
    # Connect to Snowflake using the given credentials.
    snow_engine = snow_connect(snow_credential)

    # Read the data from Snowflake using the given SQL query.
    df = pd.read_sql(query, snow_engine)

    # Close the Snowflake connection.
    snow_engine.dispose()

    # Return the extracted data as a Pandas DataFrame.
    return df


def snow_stream_extract(snow_credential, query, chunksize = 50000):
    """Stream Extract data from snowflake using only select query

    Parameters:
        snow_credential (dict): Dictionary containing Snowflake credentials and details.
        query (str): SQL query to extract data from Snowflake.
        chunksize (int): The number of rows to fetch from Snowflake in each iteration.

    Returns:
        df: Extracted data as Pandas DataFrame iterator.
    """
    # Connect to Snowflake using snow_connect function and pass snow_credential as argument
    snow_engine = snow_connect(snow_credential).connect().execution_options(stream_results=True)

    # Read the SQL query results into a Pandas dataframe using the snow_engine connection
    df = pd.read_sql(query, snow_engine, chunksize=chunksize)

    # Return the Pandas dataframe
    return df

def unload_to_s3(df, unload_path, mode='append'):
    """
    Put data to S3, mode available is append and overwrite

    Parameters:
        df (pandas.DataFrame): The data to be written to S3
        unload_path (str): The S3 path to write the data to
        mode (str): The mode for writing the data to S3, either 'append' or 'overwrite', default is 'append'
    Returns:
        None
    """
    # converting column names to uppercase
    df.columns = df.columns.str.upper()

    # checking if the dataframe is not empty
    if not df.empty:
        # writing the dataframe to a parquet file in s3
        wr.s3.to_parquet(
            df=df,
            path=unload_path,
            index=False,
            dataset=True,
            mode=mode
        )