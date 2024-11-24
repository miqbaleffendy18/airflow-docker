from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "iqbal",
    "depends_on_past": False,
    "email": ['iqbal.effendy@evermos.com'],
    'retries': 0
}

job_name = 'etl_daily_fl_snowflake_to_clickhouse'

dag = DAG(
    job_name,
    default_args = default_args,
    schedule_interval = None,
    start_date = datetime(2024, 1, 1),
    max_active_runs = 1,
    catchup = False,
    tags=['source:snowflake', 'destination:clickhouse', 'kubernetes', 'full load']
)

with dag:

    run_task = BashOperator(
        task_id=f"task_{job_name}",
        env={
            # snowflake
            'snow_user': Variable.get("etl_snow_user"),
            'snow_password': Variable.get("etl_snow_password"),
            'snow_account': Variable.get("etl_snow_account"),
            'snow_schema': Variable.get("etl_snow_schema"),
            'snow_wh': Variable.get("etl_snow_wh"),
            'snow_role': Variable.get("etl_snow_role"),
            'snow_db': Variable.get("etl_evm_snow_db"),
            'query': f"""SELECT S_DATABASE, S_SCHEMA, S_TABLE, SORTKEY
            FROM CONFIG.ETL.ETL_CONF_V2 WHERE ACTIVE = TRUE; """,
            #aws
            'AWS_ACCESS_KEY_ID': Variable.get("etl_aws_access_secret"),
            'AWS_SECRET_ACCESS_KEY': Variable.get("etl_aws_secret_key"),
            'AWS_DEFAULT_REGION': Variable.get("etl_aws_region"),
            # clickhouse
            'clickhouse_host': Variable.get("etl_clickhouse_host"),
            'clickhouse_username': Variable.get("etl_clickhouse_username"),
            'clickhouse_port': Variable.get("etl_clickhouse_port")
        },
        bash_command = """
            cd /opt/airflow/include/etl/ && \
            python fl_snowflake_to_clickhouse.py
            """
    )

    run_task