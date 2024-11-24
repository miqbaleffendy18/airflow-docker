import pandas as pd
# from sqlalchemy import create_engine, text
import clickhouse_connect
import os

def db_connect(clickhouse_credentials):
    
    clickhouse_engine = clickhouse_connect.get_client(
        host = clickhouse_credentials['host'],
        port = clickhouse_credentials['port'],
        username = clickhouse_credentials['username']
    )

    return clickhouse_engine

def execute_query(clickhouse_credentials, query):

    clickhouse_engine = db_connect(clickhouse_credentials)
    clickhouse_engine.command(query)
    clickhouse_engine.close()

def ingest_from_s3(clickhouse_credentials, db_name, table_name, aws_credentials, col_name):

    access_key = aws_credentials['access_key']
    secret_key = aws_credentials['secret_key']
    region = aws_credentials['region']
    bucket_name = aws_credentials['bucket_name']
    folder_name = aws_credentials['folder_name']
    load_path = f'https://{bucket_name}.s3.{region}.amazonaws.com/{folder_name}/TEMP_{db_name}_{table_name}.parquet/*' #Clickhouse needs https protocol

    query = f"""
    INSERT INTO {db_name}.{table_name} ({col_name})
        SELECT * 
        FROM s3Cluster(
            'default',
            '{load_path}',
            '{access_key}',
            '{secret_key}',
            'Parquet'
        )
    """
    execute_query(clickhouse_credentials, query=query)

