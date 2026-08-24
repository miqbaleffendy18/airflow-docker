import os
import pandas as pd
from utils.snow_etl_v2 import snow_extract, snow_connect

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
# Normalize "None" strings to actual NaN once
df_init.replace("None", pd.NA, inplace=True)

def sync_table_comment():
    snow_engine = snow_connect(snow_credential)
    conn = snow_engine.connect()

    df = (
        df_init[
            df_init["table_catalog"].notna() &
            df_init["log_table_comment"].notna() &
            (df_init["table_comment"] != df_init["log_table_comment"])
        ][[
            "table_catalog",
            "table_schema",
            "table_name",
            "table_comment",
            "log_table_comment"
        ]]
        .drop_duplicates()
    )

    if not df.empty:
        for _, row in df.iterrows():
            try:
                log_table_comment = row['log_table_comment'].replace("'", "''")
                query = f"""
                    ALTER TABLE "{row['table_catalog']}"."{row['table_schema']}"."{row['table_name']}"
                    SET COMMENT = '{log_table_comment}';
                """
                print(f'Updating comment for table {row["table_catalog"]}.{row["table_schema"]}.{row["table_name"]}')
                # print(query)
                conn.execute(query)
            except Exception as e:
                print(f'Error updating comment for table {row["table_catalog"]}.{row["table_schema"]}.{row["table_name"]}: {e}')
        
    
    snow_engine.dispose()

def sync_column_comment():
    snow_engine = snow_connect(snow_credential)
    conn = snow_engine.connect()

    df = (
        df_init[
            df_init["table_catalog"].notna() &
            df_init["log_column_comment"].notna() &
            (df_init["column_comment"] != df_init["log_column_comment"])
        ][[
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "column_comment",
            "log_column_comment"
        ]]
        .drop_duplicates()
    )

    if not df.empty:
        for _, row in df.iterrows():
            try:
                log_column_comment = row['log_column_comment'].replace("'", "''")   
                query = f"""
                    ALTER TABLE "{row['table_catalog']}"."{row['table_schema']}"."{row['table_name']}"
                    MODIFY COLUMN "{row['column_name']}" COMMENT '{log_column_comment}';
                """
                print(f'Updating comment for column {row["column_name"]} in table {row["table_catalog"]}.{row["table_schema"]}.{row["table_name"]}')
                # print(query)
                conn.execute(query)
            except Exception as e:
                print(f'Error updating comment for column {row["column_name"]} in table {row["table_catalog"]}.{row["table_schema"]}.{row["table_name"]}: {e}')
        
    
    snow_engine.dispose()

if __name__ == '__main__':
    sync_table_comment()
    sync_column_comment()