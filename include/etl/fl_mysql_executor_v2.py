from utils.snow_etl_v2 import create_temp_table, snow_extract, get_target_column, full_load, unload_to_s3, to_log
from utils.etl import database_stream, database_extract, mysql_information_schema
from datetime import datetime
import pandas as pd
import awswrangler as wr
import time
import os

start = time.time()

source_credential = {
    'vendor': 'mysql+pymysql',
    'host': os.environ['host'],
    'user': os.environ['user'],
    'password': os.environ['password'],
    'database': os.environ['database'],
    'port': os.environ['port'],
    'additional': 'charset=utf8mb4'
}

snow_credential = {
    'snow_user': os.environ['snow_user'],
    'snow_key_path' : os.environ['snow_keypath'],
    'snow_account': os.environ['snow_account'],
    'snow_db': os.environ['snow_db'],
    'snow_schema': os.environ['snow_schema'],
    'snow_wh': os.environ['snow_wh'],
    'snow_role': os.environ['snow_role']
}

query_init = os.environ.get("query")
df_init = snow_extract(snow_credential=snow_credential, query=query_init)

for index, row in df_init.iterrows():
    try:
        db_source = row['s_database'].lower()
        db_target = row['d_database']
        schema_source = row['s_schema'].lower()
        schema_target = row['d_schema']
        table_source = row['s_table'].lower()
        table_target = row['d_table']

        temp_table = f'TEMP_{schema_target}_{table_target}'
        temp_schema = 'TEMP_ETL'

        unload_path = f's3://evm-etl/prod/TEMP_{schema_target}_{table_target}.parquet'

        list_column = get_target_column(snow_credential=snow_credential, db=db_target, schema=schema_target, table=table_target, dbtype='mysql')
        #select from source
        query = f""" SELECT {list_column} FROM `{schema_source}`.`{table_source}`; """
        #insert log to DB
        started_at = datetime.now()
        src_count = f"""select count(*) row_count from `{schema_source}`.`{table_source}`"""
        df_src_count = database_extract(credential=source_credential, query=src_count)
        try:
            information_schema = mysql_information_schema(credential=source_credential, database=schema_source, table=table_source)
        except Exception as e:
            print(f'Warning: failed to capture information_schema: {e}', flush=True)
            information_schema = None

        print(f'Processing Table {db_target}.{schema_target}.{table_target}', flush=True)
        df = database_stream(credential=source_credential, query=query, chunksize=50000)
        # deleting object in s3 before full load
        wr.s3.delete_objects(unload_path)
        total_row = df_src_count['row_count'].to_string(index=False)

        processed_rows = 0

        for df_item in df:
            num_rows = len(df_item)
            processed_rows += num_rows
            print(f'{processed_rows} of {total_row} rows extracted and pushed to S3')
            # Transformation here
            df_item = df_item.convert_dtypes()

            date_columns = df_item.select_dtypes(include=['timedelta64']).columns.tolist()
            df_item[date_columns] = df_item[date_columns].astype(str)
            df_item[date_columns] = df_item[date_columns].replace(r"\d+ days ([\d:]+)", r"\1", regex=True)
            df_item[date_columns] = df_item[date_columns].replace("NaT", None)

            to_obj = df_item.select_dtypes(include=['object']).columns.tolist()
            df_item[to_obj] = df_item[to_obj].astype(str)
            df_item[to_obj] = df_item[to_obj].replace("None", None)
            df_item[to_obj] = df_item[to_obj].replace("0000-00-00 00:00:00", "1970-01-01 00:00:00")

            to_str = df_item.select_dtypes(include=['string']).columns.tolist()
            df_item[to_str] = df_item[to_str].replace("0000-00-00 00:00:00", "1970-01-01 00:00:00")

            unload_to_s3(unload_path=unload_path, df=df_item)

        create_temp_table(snow_credential=snow_credential,
                        db=db_target,
                        schema_name=schema_target,
                        table_name=table_target,
                        temp_schema_name=temp_schema,
                        temp_table_name=temp_table,
                        load_path=f'@s3_etl/prod/TEMP_{schema_target}_{table_target}.parquet/')

        full_load(snow_credential=snow_credential,
                db=db_target,
                schema_name=schema_target,
                table_name=table_target,
                temp_schema_name=temp_schema,
                temp_table_name=temp_table)

        completed_at = datetime.now()

        tgt_count = f"""SELECT COUNT (*) row_count FROM "{db_target}"."{schema_target}"."{table_target}" """
        df_tgt_count = snow_extract(snow_credential=snow_credential, query=tgt_count)

        df_log = {
            'name': 'EVERMOS',
            'schema_name': schema_target,
            'table_name': table_target,
            'group_id': row['dag_name_2'],
            'method': row['method'],
            'source_count': df_src_count['row_count'],
            'target_count': df_tgt_count['row_count'],
            'started_at': started_at,
            'completed_at': completed_at,
            'information_schema': information_schema
            # 'duration' : 0,
        }

        to_log(snow_credential=snow_credential, df=pd.DataFrame(data=df_log, index=[0]))
    except Exception as e:
        started_at = datetime.now()

        df_log = {
            'name': 'EVERMOS',
            'schema_name': schema_target,
            'table_name': table_target,
            'group_id': row['dag_name_2'],
            'method': row['method'],
            'started_at': started_at,
            'error_message': str(e)
            # 'duration' : 0,
        }
        to_log(snow_credential=snow_credential, df=pd.DataFrame(data=df_log, index=[0]))
        print('finish logging to db')
        
        raise #return error to get email notification


end = time.time()
print(f'Running for {round(end - start)}s')
