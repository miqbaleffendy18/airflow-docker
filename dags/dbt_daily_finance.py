import os
import sys
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes.client import models as k8s
from include.dbt_prefix_command.generate_command import get_dbt_command


#========  Notification for google chat space '[ALT] Data Build'=======#
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from modules.google_chat_notif_dbt import task_fail_alert_dbt
#======== end notification =============#


default_args = {
    "owner": "iqbal.effendy@evermos.com",
    "depends_on_past": False,
    "email": ['iqbal.effendy@evermos.com'],
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'on_failure_callback':task_fail_alert_dbt,
    'email_on_failure': True,
    'email_on_retry': False
}

job_name = "dbt-daily-finance"

dag = DAG(
    job_name,
    default_args = default_args,
    schedule_interval = "5 11 * * 1-5",
    start_date = datetime(2026, 1, 1),
    max_active_runs = 1,
    tags=['finance_closing'],
    catchup = False
)

compute_resources=k8s.V1ResourceRequirements(
    requests={
        'memory': '256Mi',
        'cpu': '500m'
    },
    limits={
        'memory': '1Gi',
        'cpu': '1000m'
    }
)

prod_mode = Variable.get("etl_env_prod")
if prod_mode == 'prod':
    env_ = 'prod'
    etl_repo = 'dataflow'
else:
    env_ = 'dev'
    etl_repo = 'dataflow-dev'


etl_giturl = 'git clone https://evermosbot:' + Variable.get("git_password") + f'@github.com/evermos/{etl_repo}'
dbt_giturl = 'git clone https://evermosbot:' + Variable.get("git_password") + '@github.com/evermos/shd-data'
dbt_target = 'finance_audit_prod'
_, dbt_failure_handling = get_dbt_command(gitclone=dbt_giturl, target=dbt_target, dbt_directory="shd-data/snowflake_dbt") 

with dag:
    dbt_build = KubernetesPodOperator(
        task_id="dbt_build",
        namespace='data',
        image=Variable.get("SNOWFLAKE_DBT_FINANCE_IMAGE"),
        labels={"pod": f"{job_name}_pod"},
        name=f"airflow_{job_name}",
        cmds=["bash", "-cx"],
        env_vars={
            'DBT_SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'DBT_ENV_SECRET_SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_DB_ACCOUNT"),
            'DBT_SNOWFLAKE_WH': 'FINANCE_ENGINE',
            'DBT_PROFILES_DIR': '/tmp/shd-data/snowflake_dbt',
            'DBT_SNOWFLAKE_PII_HASH_SALT': Variable.get("SNOWFLAKE_PII_HASH_SALT"),
            'AWS_DEFAULT_REGION': Variable.get("etl_aws_region"),
        },
        arguments=[
            f"""
            cd /tmp;
            {dbt_giturl};
            cd shd-data/snowflake_dbt/aegis;
            python aegis.py;
            cd ..
            export DBT_SNOWFLAKE_PRIVATE_KEY_PATH="/tmp/shd-data/snowflake_dbt/aegis/aegis.pem"
            dbt deps --target {dbt_target};
            dbt build --target {dbt_target} --select +fact_closing_ledger+ --exclude config.materialized:view resource_type:seed \\"""
            + dbt_failure_handling
        ],

        in_cluster=True,  # if set to true, will look in the cluster, if false, looks for file
        service_account_name=Variable.get("etl_aws_service_account"),
        container_resources=compute_resources,
        is_delete_operator_pod=True,
        node_selector={"evermos.com/serviceClass": Variable.get("NODE_SELECTOR_KUBEPOD")},
        tolerations=[k8s.V1Toleration(key="etl", operator="Equal", value="true")],
        get_logs=True,
        dag=dag
    )

    export_data_to_gsheet = KubernetesPodOperator(
        task_id="export_data_to_gsheet",
        namespace='data',
        image=Variable.get("gsheet_etl_image"),
        labels={"pod": f"{job_name}_pod"},
        name=f"airflow_{job_name}",
        cmds=["bash", "-cx"],
        env_vars={
            # snowflake
            'snow_user': Variable.get("etl_snow_user"),
            'snow_password': Variable.get("etl_snow_password"),
            'snow_account': Variable.get("etl_snow_account"),
            'snow_schema': Variable.get("etl_snow_schema"),
            'snow_wh': Variable.get("etl_snow_wh"),
            'snow_role': Variable.get("etl_snow_role"),
            'snow_db': Variable.get("etl_evm_snow_db"),
            'snow_keypath': Variable.get("etl_snow_keypath"),
            #aws
            'AWS_DEFAULT_REGION': Variable.get("etl_aws_region"),
            'EVERMOS_DATA_SERVICE_ACCOUNT': Variable.get("EVERMOS_DATA_SERVICE_ACCOUNT_SECRET"),
            #job type of execution
            'JOB_TYPE': "finance_closing"
        },
        arguments=[
            f"""cd /tmp;
            {etl_giturl};
            cd {etl_repo}/include/etl;
            pwd;
            python export_data_to_gsheet.py"""
        ],
        in_cluster=True,  # if set to true, will look in the cluster, if false, looks for file
        service_account_name=Variable.get("etl_aws_service_account"),
        container_resources=compute_resources,
        is_delete_operator_pod=True,
        node_selector={"evermos.com/serviceClass": Variable.get("NODE_SELECTOR_KUBEPOD")},
        tolerations=[k8s.V1Toleration(key="etl", operator="Equal", value="true")],
        get_logs=True,
        dag=dag
    )

    bash_commands_config = f"""
        aws eks --region ap-southeast-1 update-kubeconfig --name evermos-{env_}
    """
    task_config = BashOperator(
        task_id='task_config',
        bash_command=bash_commands_config,
        dag=dag
    )


    task_config >> dbt_build >> export_data_to_gsheet
