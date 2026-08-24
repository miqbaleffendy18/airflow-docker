def get_dbt_command(gitclone: str, dbt_directory: str, target: str):

    dbt_prefix = f"""
        cd /tmp
        n=0
        until [ $n -ge 3 ]
        do
            echo "Attempting git clone $n"
            {gitclone} && break
            n=$((n+1))
            echo "git clone failed, retrying in 30s..."
            sleep 30
        done
        if [ $n -eq 3 ]; then
            echo "git clone failed after retries"
            exit 1
        fi;

        cd {dbt_directory}/aegis;
        python aegis.py;
        cd ..
        export DBT_SNOWFLAKE_PRIVATE_KEY_PATH="/tmp/{dbt_directory}/aegis/aegis.pem"
        dbt deps --target {target};
    """

    dbt_failure_handling = f"""
        || export DBT_FAILED=1;
        aws s3 cp /tmp/{dbt_directory}/target/run_results.json s3://evm-etl/dbt/{{{{ dag.dag_id }}}}/run_results.json;
        if [ "$DBT_FAILED" = "1" ]; then exit 1; fi;
    """

    return dbt_prefix, dbt_failure_handling