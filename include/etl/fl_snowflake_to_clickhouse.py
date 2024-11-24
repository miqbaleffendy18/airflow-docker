from utils.snow_etl_v2 import snow_extract, snow_stream_extract, unload_to_s3
from utils.clickhouse_etl import execute_query, ingest_from_s3
from sqlalchemy import text
import awswrangler as wr
import time
import os

start = time.time()

snow_credential = {
    'snow_user': os.environ['snow_user'],
    'snow_password' : os.environ['snow_password'],
    'snow_account': os.environ['snow_account'],
    'snow_db': os.environ['snow_db'],
    'snow_schema': os.environ['snow_schema'],
    'snow_wh': os.environ['snow_wh'],
    'snow_role': os.environ['snow_role']
}

clickhouse_credentials = {
    'host': os.environ['clickhouse_host'],
    'username': os.environ['clickhouse_username'],
    'port': os.environ['clickhouse_port']
}

aws_credentials = {
    'access_key': os.environ['AWS_ACCESS_KEY_ID'],
    'secret_key': os.environ['AWS_SECRET_ACCESS_KEY'],
    'region': os.environ['AWS_DEFAULT_REGION'],
    'bucket_name': os.environ['AWS_BUCKET_NAME'],
    'folder_name': os.environ['AWS_FOLDER_NAME']
}

query_init = os.environ.get("query")
df_init = snow_extract(snow_credential=snow_credential, query=query_init)

for index, row in df_init.iterrows():
    db_source = row['s_database']
    schema_source = row['s_schema']
    table_source = row['s_table']
    sortkey = row['sortkey']

    # Create table in Clickhouse
    query_columns = f"""
    WITH tables as (
    select
    COLUMN_NAME,
    concat('"',UPPER(COLUMN_NAME),'"') as COLUMN_NAME_QUOTED,
    CASE
    WHEN DATA_TYPE = 'TEXT' THEN 'String'
    WHEN DATA_TYPE = 'NUMBER' THEN 'Int32'
    WHEN DATA_TYPE = 'FLOAT' THEN 'Float64'
    WHEN DATA_TYPE = 'DATE' THEN 'Date'
    WHEN DATA_TYPE IN ('TIMESTAMP_TZ', 'TIMESTAMP_NTZ') THEN 'DateTime'
    WHEN DATA_TYPE = 'TIME' THEN 'String'
    WHEN DATA_TYPE = 'BOOLEAN' THEN 'Bool'
    END AS COLUMNS,
    CASE WHEN IS_NULLABLE = 'NO' THEN 'NOT NULL' END IS_NULLABLE,
    ORDINAL_POSITION
    from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '{schema_source}' and TABLE_NAME = '{table_source}')
    select concat(COLUMN_NAME_QUOTED,' ', COLUMNS,' ', COALESCE(IS_NULLABLE,'')) as COL, COLUMN_NAME from tables ORDER BY ordinal_position
    """

    df_columns = snow_extract(snow_credential=snow_credential, query=query_columns)
    cols_quoted = list(df_columns['col'])
    curate_cols = ','.join([str(n) for n in cols_quoted]) 
    drop_ddl = f'DROP TABLE IF EXISTS "{schema_source}"."{table_source}"'
    create_ddl = f'CREATE TABLE "{schema_source}"."{table_source}"({curate_cols}) ENGINE = MergeTree() ORDER BY "{sortkey}"'
    
    execute_query(clickhouse_credentials, query=drop_ddl)
    execute_query(clickhouse_credentials, query=create_ddl)
    print(f'Table {schema_source}.{table_source} Created Successfully')

    # Extract from Snowflake
    query = f'SELECT * FROM {db_source}.{schema_source}.{table_source}'
    print(f'Processing Table {db_source}.{schema_source}.{table_source}', flush=True)
    results = snow_stream_extract(snow_credential=snow_credential, query=query, chunksize=50000)

    # Unload to S3
    unload_path = f's3://{aws_credentials["bucket_name"]}/{aws_credentials["folder_name"]}/TEMP_{schema_source}_{table_source}.parquet/'
    wr.s3.delete_objects(unload_path)
    for result_item in results:
        unload_to_s3(unload_path=unload_path, df=result_item)
    print(f'Table {schema_source}.{table_source} Unload to S3 Successfully')

    # Ingest from S3 to Clickhouse
    cols_name = list(df_columns['column_name'])
    curate_cols_name = ','.join([str(n) for n in cols_name])
    ingest_from_s3(clickhouse_credentials, db_name=schema_source, table_name=table_source, aws_credentials=aws_credentials, col_name=curate_cols_name)
    print(f'Table {schema_source}.{table_source} Ingested to Clickhouse Successfully')
    

end = time.time()
print(f'Running for {round(end - start)}s')