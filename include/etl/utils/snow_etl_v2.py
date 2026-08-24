import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
import awswrangler as wr
import base64
import boto3
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


def read_and_decrypt_key(encrypted_key_path):
    """
    Read and decrypt the private key using AWS KMS.

    :param encrypted_key_path: Path to the file containing the encrypted private key.
    :return: Decrypted private key as an RSAPrivateKey object.
    """
    # Read the encrypted private key from file
    with open(encrypted_key_path, 'rb') as file:
        encrypted_key = file.read()

    # Decrypt the private key using AWS KMS
    kms_client = boto3.client('kms')
    response = kms_client.decrypt(
        CiphertextBlob=base64.b64decode(encrypted_key)
    )

    # Load the decrypted private key as an RSAPrivateKey object
    aegis = serialization.load_pem_private_key(
        response['Plaintext'],
        password=None,
        backend=default_backend()
    )

    return aegis


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
    snow_password = snow_credential.get('snow_password')
    snow_key_path = snow_credential.get('snow_key_path')


    # Create the Snowflake URL using the extracted credentials
    url = URL(user=snow_user, account=snow_account, database=snow_db, schema=snow_schema,
              warehouse=snow_wh, role=snow_role)
    
    if snow_password:
        connect_args = {
            'password': snow_password,
        }
    elif snow_key_path:
        aegis_key = read_and_decrypt_key(snow_key_path)
        connect_args = {
            'private_key': aegis_key,
        }
    else:
        raise ValueError("Either snow_key_path or snow_password must be provided in the credentials.")


    # Create the Snowflake engine using the URL
    snow_engine = create_engine(url, connect_args=connect_args)

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


def get_target_column(snow_credential, db, schema, table, dbtype='mysql'):
    """Get the target column name(s) from the specified table in the specified schema.

    Parameters:
        snow_credential (object): Snowflake connection object.
        schema (str): Snowflake schema name.
        table (str): Snowflake table name.
        dbtype (str, optional): Database type, either 'mysql' or 'postgresql'. Default is 'mysql'.
    Returns:
        str: A string of comma-separated target column names, surrounded by either quotes or backticks depending on the specified database type.
        """
    # Connect to Snowflake database
    snow_engine = snow_connect(snow_credential)

    # Define query to retrieve data from specified table
    query = f"""SELECT * FROM "{db}"."{schema}"."{table}" LIMIT 1"""

    # Read data into a dataframe
    df = pd.read_sql(query, snow_engine)

    # Determine appropriate quote character based on database type
    if dbtype == 'postgresql':
        quote = '"'
    else:
        quote = '`'

    # Get columns of the dataframe
    cols = list(df.columns)

    # Format columns for use in SQL query
    curate_cols = ','.join([str(n) for n in [quote + s.lower() + quote for s in cols]])

    # Dispose of Snowflake engine
    snow_engine.dispose()

    # Return the formatted columns
    return curate_cols

def get_table_meta(snow_credential, db, schema, table, date_column, dbtype='mysql'):
    """Get target column list and MAX date in a single Snowflake connection.

    Combines get_target_column and get_max_date to avoid opening two separate
    connections per table in the incremental executor pool.

    Parameters:
        snow_credential (dict): Snowflake credentials.
        db (str): Snowflake database name.
        schema (str): Snowflake schema name.
        table (str): Snowflake table name.
        date_column (str): Name of the date/timestamp column (case-insensitive).
        dbtype (str): Database type, either 'mysql' or 'postgresql'. Default is 'mysql'.
    Returns:
        tuple: (list_column, max_date)
            list_column (str): Comma-separated source column list with appropriate quoting.
            max_date (str or None): MAX date as 'YYYY-MM-DD', or None if the table is empty.
    """
    snow_engine = snow_connect(snow_credential)

    df_cols = pd.read_sql(f'SELECT * FROM "{db}"."{schema}"."{table}" LIMIT 1', snow_engine)
    quote = '"' if dbtype == 'postgresql' else '`'
    cols = list(df_cols.columns)
    list_column = ','.join([quote + s.lower() + quote for s in cols])

    df_max = pd.read_sql(
        text(f'SELECT MAX({date_column.upper()}) AS max_date FROM "{db}"."{schema}"."{table}"'),
        snow_engine
    )

    snow_engine.dispose()

    raw = df_max['max_date'].iloc[0]
    max_date = None if (raw is None or pd.isnull(raw)) else pd.Timestamp(raw).date().isoformat()

    return list_column, max_date

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


def create_temp_table(snow_credential, db, schema_name, table_name, temp_schema_name, temp_table_name, load_path):
    """Create temporary table in Snowflake with the same structure as the original table.

        Parameters:
            snow_credential (dict): Snowflake connection credentials.
            schema_name (str): Name of the original schema.
            table_name (str): Name of the original table.
            temp_schema_name (str): Name of the temporary schema.
            temp_table_name (str): Name of the temporary table.
            load_path (str): S3 path where the data is stored.
        Returns:
            None
        """
    snow_engine = snow_connect(snow_credential)
    conn = snow_engine.connect()
    query = f"""SELECT * FROM "{db}"."{schema_name}"."{table_name}" LIMIT 1"""
    df = pd.read_sql(query, snow_engine)
    df.columns = df.columns.str.upper()
    print(f'Loading S3 to {table_name}')
    cols = list(df.columns)
    curate_cols = ','.join([str(n) for n in [f'$1:"{s}"' for s in cols]])
    create_temp = f"""CREATE OR REPLACE TABLE {db}.{temp_schema_name}.{temp_table_name} CLONE "{db}"."{schema_name}"."{table_name}";"""
    trun_table = f"""TRUNCATE TABLE {db}.{temp_schema_name}.{temp_table_name}"""
    query_load = f"""COPY INTO {db}.{temp_schema_name}.{temp_table_name} FROM (
            SELECT {curate_cols}
            FROM {load_path})
            FILE_FORMAT = (TYPE = 'PARQUET'); """
    print(query_load)
    conn.execute(create_temp)
    conn.execute(trun_table)
    conn.execute(query_load)
    print('Data Load to Temp Table Finished')
    snow_engine.dispose()

def create_temp_table_from_stage(snow_credential, db, schema_name, table_name, temp_schema_name, temp_table_name, stage_name, pattern):
    """Create temporary table from Stage in Snowflake with the same structure as the original table.

        Parameters:
            snow_credential (dict): Snowflake connection credentials.
            schema_name (str): Name of the original schema.
            table_name (str): Name of the original table.
            temp_schema_name (str): Name of the temporary schema.
            temp_table_name (str): Name of the temporary table.
            load_path (str): S3 path where the data is stored.
        Returns:
            None
        """
    snow_engine = snow_connect(snow_credential)
    conn = snow_engine.connect()
    query = f"""SELECT * FROM "{db}"."{schema_name}"."{table_name}" LIMIT 0"""
    df = pd.read_sql(query, snow_engine)
    df.columns = df.columns.str.upper()
    print(f'Loading S3 to {table_name}')
    create_temp = f"""CREATE OR REPLACE TABLE {db}.{temp_schema_name}.{temp_table_name} CLONE "{db}"."{schema_name}"."{table_name}";"""
    trun_table = f"""TRUNCATE TABLE {db}.{temp_schema_name}.{temp_table_name}"""
    query_load = f"""COPY INTO {db}.{temp_schema_name}.{temp_table_name} FROM @{stage_name}
        PATTERN = '{pattern}'
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE; """
    print(query_load)
    conn.execute(create_temp)
    conn.execute(trun_table)
    conn.execute(query_load)
    print('Data Load to Temp Table Finished')
    snow_engine.dispose()

def full_load(snow_credential, db, schema_name, table_name, temp_schema_name, temp_table_name):
    """Perform a full load of data from a temporary table to the final table.

    Parameters:
        snow_credential (dict): Credentials to connect to Snowflake.
        schema_name (str): Name of the schema that contains the final table.
        table_name (str): Name of the final table.
        temp_schema_name (str): Name of the schema that contains the temporary table.
        temp_table_name (str): Name of the temporary table.
    Returns:
        None
    """
    # Connect to Snowflake using the provided credentials
    snow_engine = snow_connect(snow_credential)
    # Create a connection object
    conn = snow_engine.connect()

    # SQL statements to rename the existing table and the new temporary table
    switch_out = f"""ALTER TABLE "{db}"."{schema_name}"."{table_name}" RENAME TO {db}.{temp_schema_name}.PREV_{schema_name}_{table_name}; """
    switch_in = f"""ALTER TABLE {db}.{temp_schema_name}.{temp_table_name} RENAME TO "{db}"."{schema_name}"."{table_name}"; """
    # SQL statement to drop the previous version of the table
    drop_prev = f"""DROP TABLE IF EXISTS {db}.{temp_schema_name}.PREV_{schema_name}_{table_name}"""

    # Drop the previous version of the table
    conn.execute(drop_prev)
    print(f'Backup Table {table_name} to PREV_{schema_name}_{table_name}')

    # Rename the existing table to a backup table
    conn.execute(switch_out)
    print(f'Switch in {temp_table_name} to {table_name}')

    # Rename the new temporary table to the original table name
    conn.execute(switch_in)
    print('Data Load Finished')

    # Close the connection and dispose of the engine
    snow_engine.dispose()


def upsert(snow_credential, db, schema_name, temp_schema_name, table_name, temp_table_name, left_id='id', right_id='id', insert_only=False):
    """Upsert function updates the target Snowflake table with data from the temp table.

    Parameters:
        snow_credential (str): Snowflake connection string
        schema_name (str): name of the target table's schema
        temp_schema_name (str): name of the temp table's schema
        table_name (str): name of the target table
        temp_table_name (str): name of the temp table
        left_id (str, optional): name of the column in the target table to use for comparison with the temp table's right_id column. Default is 'id'
        right_id (str, optional): name of the column in the temp table to use for comparison with the target table's left_id column. Default is 'id'
        insert_only (bool, optional): if True, only inserts data without deleting existing records. Default is False.
    Returns:
        None
    """
    snow_engine = snow_connect(snow_credential)
    conn = snow_engine.connect()
    delete_query = f"""DELETE FROM "{db}"."{schema_name}"."{table_name}" 
    WHERE {left_id} IN (SELECT {right_id} FROM {db}.{temp_schema_name}.{temp_table_name})"""
    insert_query = f"""INSERT INTO "{db}"."{schema_name}"."{table_name}" SELECT *
    FROM {db}.{temp_schema_name}.{temp_table_name} """
    if not insert_only:
        print('Executing delete query')
        conn.execute(delete_query)
    print(f'Upsert temp table to target')
    conn.execute(insert_query)
    print("Data loaded successful")
    snow_engine.dispose()


def to_log(snow_credential, df):
    """Insert a log of the dataframe into Snowflake database

    Parameters:
        snow_credential (dict): A dictionary of credentials for connecting to Snowflake. It should contain the following keys:
            'snow_user': Snowflake username
            'snow_password': Snowflake password
            'snow_account': Snowflake account name
            'snow_wh': Snowflake warehouse name
            'snow_role': Snowflake role
        df (pandas.DataFrame): The dataframe to be logged
    Returns:
        None
    """
    credential = {
        'snow_user': snow_credential['snow_user'],
        'snow_account': snow_credential['snow_account'],
        'snow_db': 'CONFIG',
        'snow_schema': 'ETL',
        'snow_wh': snow_credential['snow_wh'],
        'snow_role': snow_credential['snow_role'],
        'snow_password': snow_credential.get('snow_password'),
        'snow_key_path' : snow_credential.get('snow_key_path')
    }
    snow_engine = snow_connect(credential)
    df.to_sql('etl_log', snow_engine, index=False, if_exists='append')
    snow_engine.dispose()
