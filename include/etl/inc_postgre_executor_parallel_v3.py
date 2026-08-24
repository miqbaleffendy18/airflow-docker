import time
from multiprocessing import Pool
import os
from sys import argv
from utils.snow_etl_v2 import snow_extract, upsert, get_table_meta, unload_to_s3, create_temp_table, to_log
from utils.etl import database_stream, database_extract, pg_information_schema
import numpy as np
import pandas as pd
from datetime import datetime
import awswrangler as wr

start = time.time()

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

ds = argv[1]
df_init = snow_extract(snow_credential=snow_credential, query=query_init)

def process(df_init):
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
            date_column = row['date_key']
            join_key = row['join_key']

            list_column, max_date = get_table_meta(snow_credential=snow_credential, db=db_target, schema=schema_target, table=table_target, date_column=date_column, dbtype='postgresql')
            if max_date is None or max_date > ds:
                # max_date is None (empty table) or future-dated dirty data — fall back to ds.
                effective_ds = ds
                print(f'max_date={max_date}, falling back to ds={ds}', flush=True)
            else:
                effective_ds = max_date
                print(f'Resuming from max date: {effective_ds} in {db_target}.{schema_target}.{table_target}', flush=True)

            source_credential = {
                'vendor': 'postgresql',
                'host': os.environ['host'],
                'user': os.environ['user'],
                'password': os.environ['password'],
                'database': db_source,
                'port': os.environ['port'],
                'additional': 'sslmode=require'
            }


            query = f""" SELECT {list_column} from "{schema_source}"."{table_source}"
                        where {date_column} >= '{effective_ds}'; """
            started_at = datetime.now()
            src_count = f"""select count(*) row_count from "{schema_source}"."{table_source}" """
            df_src_count = database_extract(credential=source_credential, query=src_count)
            try:
                information_schema = pg_information_schema(credential=source_credential, database=db_source, schema=schema_source, table=table_source)
            except Exception as e:
                print(f'Warning: failed to capture information_schema: {e}', flush=True)
                information_schema = None

            print(f'Processing Table {db_target}.{schema_target}.{table_target}', flush=True)
            df = database_stream(credential=source_credential, query=query, chunksize=50000)

            wr.s3.delete_objects(unload_path)
            for df_item in df:
            # Transformation Here
                df_item = df_item.convert_dtypes()
                to_str = df_item.select_dtypes(include=['object']).columns.tolist()
                df_item[to_str] = df_item[to_str].astype(str)
                df_item[to_str] = df_item[to_str].replace("None", None)

                unload_to_s3(unload_path=unload_path, df=df_item, mode='append')

            create_temp_table(snow_credential=snow_credential,
                            db=db_target,
                            schema_name=schema_target,
                            table_name=table_target,
                            temp_schema_name=temp_schema,
                            temp_table_name=temp_table,
                            load_path=f'@s3_etl/prod/TEMP_{schema_target}_{table_target}.parquet/')

            upsert(snow_credential=snow_credential,
                db=db_target,
                schema_name=schema_target,
                table_name=table_target,
                temp_schema_name=temp_schema,
                temp_table_name=temp_table,
                left_id=join_key,
                right_id=join_key)

            completed_at = datetime.now()

            tgt_count = f"""SELECT COUNT (*) row_count FROM "{db_target}"."{schema_target}"."{table_target}" """
            df_tgt_count = snow_extract(snow_credential=snow_credential, query=tgt_count)

            df_log = {
                'name': 'EVERPRO',
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
            print('finish logging to db')

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


if __name__ == '__main__':
    data_split = np.array_split(df_init, 8)
    p = Pool(8)
    p.map(process, data_split)
    p.close()

    end = time.time()
    print(f'Running for {round(end - start)}s')
