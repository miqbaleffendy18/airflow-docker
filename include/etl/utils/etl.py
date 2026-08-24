# import needed libraries
from sqlalchemy import create_engine
import pandas as pd
import time
import boto3
import base64
import json

def decrypt_key(encrypted_key: str) -> str:

    kms_client = boto3.client('kms')
    response = kms_client.decrypt(
        CiphertextBlob=base64.b64decode(encrypted_key)
    )

    decrypted_key = response['Plaintext'].decode('utf-8')
    return decrypted_key

def db_connection(credential):
    vendor = credential['vendor']
    host = credential['host']
    user = decrypt_key(credential['user'])
    password = decrypt_key(credential['password'])
    database = credential['database']
    port = credential['port']
    additional = credential['additional']
    conn_source = f'{vendor}://{user}:{password}@{host}:{port}/{database}?{additional}'
    engine_source = create_engine(conn_source)
    return engine_source


def database_extract(credential, query):
    engine_source = db_connection(credential)
    df = pd.read_sql(query, engine_source)
    print(f'{len(df)} Rows extracted and load into Dataframe')
    return df


def database_stream(credential, query,chunksize=200000):
    engine_source = db_connection(credential).connect().execution_options(stream_results = True)
    df = pd.read_sql(query, engine_source, chunksize=chunksize)
    return df


def execute_pg(credential, query):
    host = credential['host']
    user = credential['user']
    password = credential['password']
    database = credential['database']
    port = credential['port']

    conn_source = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    print('Connecting to the database . . .')
    engine_source = create_engine(conn_source)
    conn = engine_source.connect()
    conn.execute(query)
    print('Query Executed Successfully')


def extract_pg(credential, query):
    host = credential['host']
    user = credential['user']
    password = credential['password']
    database = credential['database']
    port = credential['port']

    conn_source = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    print('Connecting to the database . . .')
    engine_source = create_engine(conn_source)
    query = query

    extracted_df = pd.read_sql(query, engine_source)
    print(f'{len(extracted_df)} Rows extracted and load into Dataframe')
    return extracted_df


def extract_redshift(credential, query):
    try:
        host = credential['host']
        user = credential['user']
        password = credential['password']
        database = credential['database']
        port = credential['port']

        conn_source = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        print('Connecting to the database . . .')
        engine_source = create_engine(conn_source)
        query = query

        extracted_df = pd.read_sql(query, engine_source)
        print(f'{len(extracted_df)} Rows extracted and load into Dataframe')
        return extracted_df
    except Exception as e:
        print("Data extract error: " + str(e))


def extract_mysql(credential, query):
    host = credential['host']
    user = credential['user']
    password = credential['password']
    database = credential['database']
    port = credential['port']

    conn_source = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4'
    print('Connecting to the database . . .')
    engine_source = create_engine(conn_source)
    query = query

    extracted_df = pd.read_sql(query, engine_source)
    print(f'{len(extracted_df)} Rows extracted and load into Dataframe')
    return extracted_df


def insert(credential, df2load,schema_name, table_name, method='append'):
    host_r = credential['host']
    user_r = credential['user']
    password_r = credential['password']
    database_r = credential['database']
    port_r = credential['port']

    print(f'Loading {len(df2load)} rows to target')
    conn_target = f'postgresql://{user_r}:{password_r}@{host_r}:{port_r}/{database_r}'
    engine_target = create_engine(conn_target)
    # save df to postgres

    df2load.to_sql(table_name, engine_target, schema=schema_name, if_exists=method, index=False, method='multi',
                   chunksize=10000)
    # add elapsed time to final print out
    print("Data loaded successful")


def upsert(credential, df2load, schema_name, temp_schema_name, table_name, left_id='id', right_id='id'):
    host_r = credential['host']
    user_r = credential['user']
    password_r = credential['password']
    database_r = credential['database']
    port_r = credential['port']

    conn_target = f'postgresql://{user_r}:{password_r}@{host_r}:{port_r}/{database_r}'
    engine_target = create_engine(conn_target)
    create_temp = f"""create table if not exists {temp_schema_name}.{table_name} (like {schema_name}.{table_name})"""
    delete_query = f"""delete from {schema_name}.{table_name} 
    where {left_id} in (select {right_id} from {temp_schema_name}.{table_name})"""

    insert_query = f"""insert into {schema_name}.{table_name} select *
    from {temp_schema_name}.{table_name}"""

    # save df to postgres
    print(f'Loading {len(df2load)} rows to temporary table')
    conn = engine_target.connect()
    conn.execute(create_temp)
    conn.execute(f'truncate table {temp_schema_name}.{table_name}')

    df2load.to_sql(table_name, engine_target, schema=temp_schema_name, if_exists='append', index=False,
                   method='multi', chunksize=10000)

    conn.execute(delete_query)
    print('Upsert temp table to target')
    conn.execute(insert_query)
    print("Data loaded successful")


def large_load(credential, query, table_name, schema_name, method='append'):
    host = credential['host']
    user = credential['user']
    password = credential['password']
    database = credential['database']
    port = credential['port']

    conn_source = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4'
    print('Connecting to the database . . .')
    engine_source = create_engine(conn_source).execution_options(stream_results=True)
    query = query

    extracted_df = pd.read_sql(query, engine_source, chunksize=100000)
    start = time.time()
    for dataset in extracted_df:
        print(f'{len(dataset)} Rows extracted and load into Dataframe')
        conn_target = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
        engine_target = create_engine(conn_target)
        # save df to postgres

        dataset.to_sql(table_name, engine_target, schema=schema_name, if_exists=method, index=False, method='multi',
                       chunksize=100000)

        print("Data loaded successful")
    end = time.time()
    print(f'Running for {round(end - start)}s')



def db_connection_with_auto_commit(credential):
    vendor = credential['vendor']
    host = credential['host']
    user = decrypt_key(credential['user'])
    password = decrypt_key(credential['password'])
    database = credential['database']
    port = credential['port']
    additional = credential['additional']
    conn_source = f'{vendor}://{user}:{password}@{host}:{port}/{database}?{additional}'
    engine_source = create_engine(conn_source,isolation_level="AUTOCOMMIT")
    return engine_source

def json_information_schema(df: pd.DataFrame) -> dict:
    """
    Input: DataFrame from database_extract() — single table only
    Output: single table dict with columns nested as a list
    """
    if df.empty:
        return {}

    # Replace NaN with None for clean JSON serialization
    df = df.where(pd.notna(df), None)

    first = df.iloc[0]

    table = {
        "table_catalog":  first["table_catalog"],
        "table_schema":   first["table_schema"],
        "table_name":     first["table_name"],
        "table_comment":  first["table_comment"],
        "columns": [
            {
                "column_name":              row["column_name"],
                "is_nullable":              row["is_nullable"],
                "data_type":                row["data_type"],
                "character_maximum_length": row["character_maximum_length"],
                "numeric_precision":        row["numeric_precision"],
                "column_comment":           row["column_comment"],
            }
            for _, row in df.iterrows()
            if row["column_name"] is not None
        ]
    }

    return json.dumps(table, default=str)

def pg_information_schema(credential, database, schema, table):
    query = f"""
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        obj_description(pc.oid, 'pg_class') AS table_comment,
        c.column_name,
        c.is_nullable,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        col_description(pc.oid, c.ordinal_position)  AS column_comment
    FROM information_schema.tables t
    LEFT JOIN information_schema.columns c
        ON t.table_schema = c.table_schema
        AND t.table_name = c.table_name
    LEFT JOIN pg_catalog.pg_class pc
        ON pc.relname = t.table_name
    LEFT JOIN pg_catalog.pg_namespace pn
        ON pn.oid = pc.relnamespace
        AND pn.nspname = t.table_schema
    WHERE 1=1
    AND t.table_catalog = '{database}'
    AND t.table_schema = '{schema}'
    AND t.table_type = 'BASE TABLE'
    AND t.table_name = '{table}'
    """

    df = database_extract(credential=credential, query=query)
    return json_information_schema(df)

def mysql_information_schema(credential, database, table):
    query = f"""
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.table_comment,
        c.column_name,
        c.is_nullable,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.column_comment
    FROM information_schema.tables t
    LEFT JOIN information_schema.columns c
        ON t.table_schema = c.table_schema
        AND t.table_name = c.table_name
    WHERE t.table_schema = '{database}'
    AND t.table_type = 'BASE TABLE'
    AND t.table_name = '{table}'
    """

    df = database_extract(credential=credential, query=query)
    return json_information_schema(df)