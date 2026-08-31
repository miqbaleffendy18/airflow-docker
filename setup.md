Welcome to Evermos dbt project!

### Requirements
1. Python 3.9
2. dbt-core 1.3 or above
3. dbt-snowflake 1.3 or above

## Set up VSCode:

1. Install VSCode from https://code.visualstudio.com/
1. Open Command Palette (Ctrl+Shift+P / Cmd+shift+P) and choose `Extensions: Show Recommended Extensions` command
1. Install all recommended extensions
1. Open Command Palette and choose `Preferences: Open Settings (JSON)` to open your `settings.json`
1. Add this value to your `settings.json`:
```json
    "files.associations": {
        "*.sql": "jinja-sql"
    }
```

## Set up python using Homebrew:
```bash
$ brew install python
$ brew link python
```

## Install dbt:
1. Install dbt using pip 
```bash
$ pip install dbt-redshift==1.3.0 dbt-snowflake==1.3.0
```
or upgrade it if you already have dbt-redshift
```bash
$ pip install --upgrade dbt-redshift==1.3.0
```
2. Make sure you have installed dbt: run `$ dbt --version`

## Set up dbt profile on MacOS:
1. Open .zprofile file at `~/.zprofile`
2. Add these lines and fill using your credentials
```bash
export DBT_REDSHIFT_HOST="redshift-cluster-1.cnqce7xb1bf5.ap-southeast-1.redshift.amazonaws.com"
export DBT_REDSHIFT_USER=
export DBT_REDSHIFT_PASS=
export DBT_REDSHIFT_DB=evmredshift
export DBT_REDSHIFT_SCHEMA=dbt_USERNAME
export DBT_SNOWFLAKE_USER=
export DBT_ENV_SECRET_SNOWFLAKE_PASS=
export DBT_ENV_SECRET_SNOWFLAKE_ACCOUNT="vh01955.ap-southeast-1"
export DBT_SNOWFLAKE_WH=COMPUTE_WH
export DBT_SNOWFLAKE_DBT_DB=DEV
export DBT_SNOWFLAKE_DBT_SCHEMA=DBT_USERNAME
export DBT_SNOWFLAKE_DBT_EVM_SCHEMA=EVM_USERNAME
export DBT_SNOWFLAKE_DBT_EVP_SCHEMA=EVP_USERNAME
```
3. Save and update the profile
```bash
$ source ~/.zprofile
```
4. Clone a dbt repo (evermos-dbt or everpro-dbt)
5. Open the repo folder and run `dbt deps` to install dbt dependencies
6. Make sure you are connected to VPN. Run `dbt debug` to test the connection

## Set up dbt profile on Windows:
1. Open VSCode or Notepad and copy these lines
```
setx DBT_REDSHIFT_HOST "redshift-cluster-1.cnqce7xb1bf5.ap-southeast-1.redshift.amazonaws.com"
setx DBT_REDSHIFT_USER "USERNAME"
setx DBT_REDSHIFT_PASS "PASSWORD"
setx DBT_REDSHIFT_DB "evmredshift"
setx DBT_REDSHIFT_SCHEMA "dbt_USERNAME"
setx DBT_SNOWFLAKE_USER "USERNAME"
setx DBT_ENV_SECRET_SNOWFLAKE_PASS "PASSWORD"
setx DBT_ENV_SECRET_SNOWFLAKE_ACCOUNT "vh01955.ap-southeast-1"
setx DBT_SNOWFLAKE_WH "COMPUTE_WH"
setx DBT_SNOWFLAKE_DBT_DB "DEV"
setx DBT_SNOWFLAKE_DBT_SCHEMA "DBT_USERNAME"
setx DBT_SNOWFLAKE_DBT_EVM_SCHEMA "EVM_USERNAME"
setx DBT_SNOWFLAKE_DBT_EVP_SCHEMA "EVP_USERNAME"
```
2. Replace USERNAME and PASSWORD using your credentials
3. Open Command Prompt (Ctrl+R, run `cmd`) and paste each lines above to the Command Prompt
4. Clone a dbt repo (evermos-dbt or everpro-dbt)
5. Open the repo folder and run `dbt deps` to install dbt dependencies
6. Make sure you are connected to VPN. Run `dbt debug` to test the connection


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
