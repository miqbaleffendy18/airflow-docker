# Dataflow Skills & Conventions

Developer reference for contributing DAGs and ETL jobs to this repository.

---

## 1. ETL Standard Executors

Any ETL task that reads its job config from `CONFIG.ETL.ETL_CONF_V2` in Snowflake **must** use one of the four approved executor scripts. Do not write a one-off script that re-implements this logic.

| Executor | Source DB | Load type |
|---|---|---|
| [include/etl/fl_postgre_executor_v2.py](include/etl/fl_postgre_executor_v2.py) | PostgreSQL | Full load (truncate + insert) |
| [include/etl/fl_mysql_executor_v2.py](include/etl/fl_mysql_executor_v2.py) | MySQL | Full load (truncate + insert) |
| [include/etl/inc_postgre_executor_parallel_v2.py](include/etl/inc_postgre_executor_parallel_v2.py) | PostgreSQL | Incremental upsert (parallel) |
| [include/etl/inc_mysql_executor_parallel_v2.py](include/etl/inc_mysql_executor_parallel_v2.py) | MySQL | Incremental upsert (parallel) |

Choose full-load (`fl_`) for tables that can be safely rebuilt on every run. Choose incremental parallel (`inc_…_parallel_v2`) for large tables where only changed rows need to be synced.

---

## 2. Custom Job Development

When the standard executors cannot satisfy the job requirements, write a custom executor script. Custom scripts **must** import from both shared utils — one for the source database side, one for the Snowflake/S3 side.

### Source database side — `etl.py`

```python
from utils.etl import (
    database_extract,           # run a SELECT, return a full DataFrame
    database_stream,            # chunked streaming extract (large tables)
    pg_information_schema,      # fetch PostgreSQL table + column metadata
    mysql_information_schema,   # fetch MySQL table + column metadata
    decrypt_key,                # KMS-decrypt an encrypted credential string
    db_connection,              # build a SQLAlchemy engine from a credential dict
)
```

File: [include/etl/utils/etl.py](include/etl/utils/etl.py)

Handles connections to MySQL and PostgreSQL source databases. Credentials are passed as a dict with keys `vendor`, `host`, `user`, `password`, `database`, `port`, `additional`.

### Snowflake & S3 side — `snow_etl_v2.py`

```python
from utils.snow_etl_v2 import (
    snow_extract,       # run a SELECT against Snowflake, return a DataFrame
    get_target_column,  # fetch target column list from Snowflake metadata
    full_load,          # truncate-insert into Snowflake
    upsert,             # merge/upsert into Snowflake via temp table
    create_temp_table,  # create a staging temp table in TEMP_ETL schema
    unload_to_s3,       # stage a DataFrame to S3 as Parquet
    to_log,             # write an ETL run log record
)
```

File: [include/etl/utils/snow_etl_v2.py](include/etl/utils/snow_etl_v2.py)

Uses AWS KMS-encrypted private key for Snowflake authentication.

### Deprecated — `snow_etl.py`

[include/etl/utils/snow_etl.py](include/etl/utils/snow_etl.py) is **deprecated**. Do not use it in new jobs. It relies on password-based Snowflake auth and lacks the helper functions available in `snow_etl_v2`.

### General-purpose S3 file access — `s3.py`

`unload_to_s3` (above) is for staging a DataFrame as Parquet during an ETL load. For anything else — uploading an arbitrary local file, fetching an object, or downloading a file such as a trained model — use the centralized S3 module instead:

```python
from include.s3_aws.s3 import (
    upload_data_to_s3,       # upload a local file to S3
    load_data_from_s3,       # fetch an object from S3
    download_model_from_s3,  # download a file (e.g. a trained model) from S3 to local disk
)
```

File: [include/s3_aws/s3.py](include/s3_aws/s3.py)

Falls back to the boto3 default credential chain (e.g. an IRSA service account token) when static AWS keys aren't set in the environment.

### Google Drive access — `gdrive_etl.py`

For ETL jobs that pull data from Google Drive, Sheets, Docs, or Slides (e.g. ingesting a shared spreadsheet), use the centralized Drive module:

```python
from include.etl.utils.gdrive_etl import (
    build_drive_service,   # authenticate and build a Drive v3 service from a service-account credential JSON
    extract_folder_id,     # parse a folder ID out of a Drive folder URL
    extract_file_id,       # parse a file ID out of a Drive/Docs/Sheets/Slides URL
    list_files,            # list files in a folder matching given MIME types
    get_owner,             # resolve the owner/last-modifier email from file metadata
    download_file,         # download a file into an in-memory buffer
    move_file,             # move a file to a different folder
    sanitize_identifier,   # sanitize a string into a valid Snowflake identifier
    derive_table_name,     # derive a Snowflake table name from a file/tab name
)
```

File: [include/etl/utils/gdrive_etl.py](include/etl/utils/gdrive_etl.py)

Uses a Google service-account credential (JSON string, typically read from a decrypted Airflow Variable) for auth.

### Always use the centralized connection libraries

Snowflake, S3, and Google Drive connection/auth logic each belong in exactly one place — `include/etl/utils/snow_etl_v2.py` for Snowflake, `include/s3_aws/s3.py` for general S3 file access, and `include/etl/utils/gdrive_etl.py` for Google Drive. Never copy this logic into a project-local module. A duplicated copy has to be patched separately every time auth methods, credentials, or bugs change, so always import from the centralized modules instead of reimplementing them.

---

## 3. DAG Template

All new DAGs must follow the standard Kubernetes DAG template. In VS Code, trigger the snippet by typing:

```
airflow-dag-kube-template
```

Snippet file: [.vscode/airflow-dag-kube-template.code-snippets](.vscode/airflow-dag-kube-template.code-snippets)

### Required fields to fill in

| Placeholder | What to put |
|---|---|
| `owner` | Your name or team name |
| `email` | Alert email address |
| `dag_name` | Unique DAG identifier (also used as `job_name`) |
| `team_tag` | Airflow tag for your team (e.g. `growth`, `commerce`) |

### Standard Airflow Variables for ETL jobs

| Purpose | Airflow Variable |
|---|---|
| Snowflake user | `etl_snow_user` |
| Snowflake warehouse | `etl_snow_wh` |
| Pod image | `etl_image` |

### Failure notifications

Wire `on_failure_callback` in `default_args` so failures post a card to the team's Google Chat space automatically. The module is at [dags/modules/google_chat_notif.py](dags/modules/google_chat_notif.py).

```python
import sys, os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from modules.google_chat_notif import task_fail_alert

default_args = {
    'owner': 'your-name',
    ...
    'on_failure_callback': task_fail_alert,
}
```

Available callbacks and which Google Chat webhook they post to:

| Callback | Airflow connection | Use when |
|---|---|---|
| `task_fail_alert` | `gchat_webhook` | General ETL / data team space |
| `task_fail_alert_ar` | `gchat_webhook_ar` | AR team space |
| `task_fail_alert_eng` | `gchat_webhook_eng` | Engineering space |
| `task_success_alert` | `gchat_webhook` | Optional success confirmation |

The callback sends a rich card (task name, DAG, execution time, duration, exception excerpt) with "View Logs" and "Mark Success" quick-action buttons.

---

### AWS authentication in KubernetesPodOperator

**Strongly advised — Kubernetes service account (IRSA)**

Use `service_account_name` so the pod inherits AWS permissions via IAM Roles for Service Accounts. No AWS keys appear in `env_vars`.

```python
# Reference: dags/etl_daily_inc_evm_2_dag.py
task_kube = KubernetesPodOperator(
    ...
    service_account_name=Variable.get("etl_aws_service_account"),
    env_vars={
        ...
        'AWS_DEFAULT_REGION': Variable.get("etl_aws_region"),
        # no AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY needed
    },
    ...
)
```

**Fallback — static AWS keys**

Use this only when a service account cannot be assigned (e.g. the pod uses a non-standard image that requires explicit credentials). Decrypt the keys with `decrypt_var` — never pass them in plaintext.

```python
# Reference: dags/etl_daily_fl_snowflake_to_clickhouse_1.py
from include.etl.utils.decrypt import decrypt_var

task_kube = KubernetesPodOperator(
    ...
    env_vars={
        ...
        'AWS_ACCESS_KEY_ID':     decrypt_var(Variable.get("etl_aws_access_secret")),
        'AWS_SECRET_ACCESS_KEY': decrypt_var(Variable.get("etl_aws_secret_key")),
        'AWS_DEFAULT_REGION':    Variable.get("etl_aws_region"),
    },
    ...
)
```

### Key structural rules

- The DAG must always include **two tasks** in this order:
  1. `task_config` — `BashOperator` that runs `aws eks update-kubeconfig` to set the active cluster context.
  2. `task_kube` — `KubernetesPodOperator` that clones the repo and runs your script.
- Wire them as `task_config >> task_kube`. Do not skip `task_config`.
- Always add `source:<name>` and `destination:<name>` tags alongside your team tag.
- Resource defaults (`512m`/`800m` request, `4Gi`/`2000m` limit) are intentional starting points — adjust only when profiling justifies it.
- `max_active_runs=1` and `catchup=False` are required on all DAGs.
- Sensitive values must go through `decrypt_var` or be read from Airflow Variables, never hardcoded.

---

## 4. Branching Strategy

Never commit directly to `airflow_dev`. Always work on a dedicated branch.

| Change type | Suggested branch name |
|---|---|
| New DAG | `feat/<dag-name>` |
| Bug fix on existing DAG | `fix/<dag-name>` |
| Refactor / housekeeping | `refactor/<description>` |

```bash
git checkout -b feat/my-new-dag
# ... make changes ...
git push origin feat/my-new-dag
```

Open a PR into `airflow_dev` when ready for review.

---

## 5. Pull Request Process

Use the repository PR template at [.github/pull_request_template.md](.github/pull_request_template.md). GitHub applies it automatically when you open a PR.

### Pre-merge checklist

- [ ] PR title contains the ticket number and a clear description
- [ ] Self-review completed — no unused imports, no hardcoded secrets
- [ ] Sensitive variables encrypted via Fernet or AWS KMS
- [ ] Hard-to-understand sections commented
- [ ] [Airflow Schedule](https://docs.google.com/spreadsheets/d/1P1NBtPKI_0i8UxcxWhMdCOuWLtpTKZCvkSjYk8aKQec/edit#gid=0) doc updated
- [ ] Code follows the [DAG Best Practices](https://docs.google.com/document/d/1ddT4hRed8jhRU7QWG41acxU7tSr9XEvHYvvJkWZx8Cg/edit?usp=sharing) document
- [ ] DAG tested in local or development environment
- [ ] Validation screenshot attached showing successful dev run
- [ ] Tags include `source:<name>` and `destination:<name>`
- [ ] DAG is **paused** on [Airflow Dev](http://airflow.dev.internal/home) before merging

---

## 6. ETL_CONF_V2 Config Schema

The standard executors and any DAG using `CONFIG.ETL.ETL_CONF_V2` drive their behaviour from rows in this Snowflake config table. Each row represents one source-to-destination table mapping.

| Column | Description |
|---|---|
| `ID` | Unique row identifier — used by `create_table_snowflake` and `manual_full_load_snowflake` DAGs to target a specific config row |
| `S_DATABASE` | Source database name |
| `S_SCHEMA` | Source schema name |
| `S_TABLE` | Source table name |
| `D_DATABASE` | Destination Snowflake database |
| `D_SCHEMA` | Destination Snowflake schema |
| `D_TABLE` | Destination Snowflake table name |
| `JOIN_KEY` | Primary/unique key column used for upsert merges (incremental jobs) |
| `DATE_KEY` | Date/timestamp column used to filter incremental rows |
| `DAG_NAME_1` | Matches the `job_name` of the DAG that owns this row |
| `DAG_NAME_2` | Secondary DAG name for parallel task splitting (incremental parallel executors) |
| `METHOD` | Load method passed to the executor (e.g. `upsert`, `full`) |
| `ACTIVE` | `TRUE` / `FALSE` — rows where `ACTIVE = FALSE` are skipped by all executors |

Every executor queries the table with a filter on `ACTIVE = TRUE AND LOWER(DAG_NAME_1) = LOWER('<job_name>')`, so `DAG_NAME_1` must exactly match the DAG's `job_name`. Add the row to the config table before deploying the DAG.

---

## 7. Encrypting Sensitive Variables

Never pass secrets as plaintext in `env_vars`. Two encryption utilities are available depending on context.

### `decrypt_var` — for DAG-level env vars (Fernet)

Use this in the DAG file when injecting a sensitive Airflow Variable into a pod's environment. The variable must first be encrypted with the Fernet key stored in the `VAR_SECRET` Airflow Variable.

```python
from include.etl.utils.decrypt import decrypt_var

env_vars={
    'AWS_ACCESS_KEY_ID':     decrypt_var(Variable.get("etl_aws_access_secret")),
    'AWS_SECRET_ACCESS_KEY': decrypt_var(Variable.get("etl_aws_secret_key")),
}
```

File: [include/etl/utils/decrypt.py](include/etl/utils/decrypt.py)

### `decrypt_key` — for runtime DB credentials (AWS KMS)

Use this inside executor scripts when decrypting source database credentials at runtime. The credential string must be KMS-encrypted.

```python
from utils.etl import decrypt_key

password = decrypt_key(os.environ['password'])
```

Defined in: [include/etl/utils/etl.py](include/etl/utils/etl.py)

### Which to use

| Scenario | Tool |
|---|---|
| Airflow Variable → pod `env_vars` in a DAG file | `decrypt_var` (Fernet) |
| DB credential stored as env var → used inside an executor script | `decrypt_key` (AWS KMS) |
| AWS credentials when service account is not available | `decrypt_var` (Fernet) |

### Encrypting new secrets with KMS — `encrypt_kms` DAG

To onboard a new KMS-encrypted secret, use [dags/encrypt_kms_dag.py](dags/encrypt_kms_dag.py). It reads a `variables.json` file from S3, encrypts the values, writes `variables_encrypted.json`, and deletes the plaintext source file.

**Step 1 — prepare `variables.json`**

Upload the file to `s3://evm-etl/dev/secrets/variables.json`. Two supported formats:

```json
{
  "variable_name": "plain_secret_string",
  "db_credential_variable": {
    "user": "db_user",
    "password": "db_password",
    "host": "...",
    "database": "..."
  }
}
```

For dict values, only `user` and `password` fields are encrypted; other fields are stored as-is.

**Step 2 — trigger the DAG**

Trigger `encrypt_kms` on Airflow Dev with `mode=encrypt`. On success, `variables_encrypted.json` appears in the same S3 path and the plaintext `variables.json` is deleted.

**Step 3 — import into Airflow**

Copy the encrypted values from `variables_encrypted.json` into the corresponding Airflow Variables.

**Re-encrypting after a KMS key rotation:** trigger the same DAG with `mode=re_encrypt`. It reads existing encrypted values and re-encrypts them under the new key.

Script: [include/etl/utils/encrypt_kms.py](include/etl/utils/encrypt_kms.py)

---

## 8. DBT Jobs

DBT DAGs share the same `task_config >> task_kube` skeleton as ETL DAGs but differ in several important ways. Reference: [dags/dbt_build_corporate.py](dags/dbt_build_corporate.py).

### Failure notification — use the DBT-specific module

DBT DAGs must use `task_fail_alert_dbt` from `modules.google_chat_notif_dbt`, **not** `task_fail_alert` from `google_chat_notif`. The DBT version parses `run_results.json` from S3 to produce a richer card that shows per-model pass/warn/error counts.

```python
import sys, os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from modules.google_chat_notif_dbt import task_fail_alert_dbt

default_args = {
    ...
    'on_failure_callback': task_fail_alert_dbt,
}
```

### Bash arguments — use `get_dbt_command()`

Never write the git-clone + aegis + dbt setup bash script by hand. Use the helper from [include/dbt_prefix_command/generate_command.py](include/dbt_prefix_command/generate_command.py) which returns two strings to compose the pod's `arguments`.

```python
from include.dbt_prefix_command.generate_command import get_dbt_command

gitclone = 'git clone --depth 1 https://evermosbot:' + Variable.get("git_password") + '@github.com/evermos/<repo>'
target = Variable.get("<env_variable>")
dbt_prefix, dbt_failure_handling = get_dbt_command(gitclone=gitclone, target=target, dbt_directory="<repo>")
```

`dbt_prefix` handles: git clone with retry (3 attempts, 30 s backoff), running `aegis.py` to decrypt the Snowflake private key, and `dbt deps`.

`dbt_failure_handling` handles: uploading `run_results.json` to `s3://evm-etl/dbt/<dag_id>/` on failure so the notification callback can parse it, then exits with code 1 to mark the task failed.

Use them in `arguments` like this:

```python
arguments=[
    dbt_prefix
    + f"dbt build --target {target} --select {{{{ dag_run.conf['models'] }}}} \\"
    + dbt_failure_handling
]
```

### Snowflake credentials and standard Airflow Variables

DBT DAGs use a different set of env var names from ETL DAGs. Pass these in `env_vars`:

```python
snowflake_credential = {
    'DBT_SNOWFLAKE_USER':                  Variable.get("SNOWFLAKE_USER"),
    'DBT_ENV_SECRET_SNOWFLAKE_ACCOUNT':    Variable.get("SNOWFLAKE_DB_ACCOUNT"),
    'DBT_SNOWFLAKE_WH':                    Variable.get("SNOWFLAKE_WH"),
    'DBT_PROFILES_DIR':                    '/tmp/<dbt_directory>',
    'DBT_SNOWFLAKE_PII_HASH_SALT':         Variable.get("SNOWFLAKE_PII_HASH_SALT"),
    'ELEMENTARY_USER':                     Variable.get("DBT_ELEMENTARY_USER"),
    'ELEMENTARY_PASSWORD':                 Variable.get("DBT_ELEMENTARY_PASSWORD"),
    'AWS_DEFAULT_REGION':                  Variable.get("etl_aws_region"),
}
```

The private key path is set automatically by `aegis.py` inside `dbt_prefix` — do not set `DBT_SNOWFLAKE_PRIVATE_KEY_PATH` manually.

Standard Airflow Variables for DBT jobs:

| Purpose | Airflow Variable |
|---|---|
| Snowflake user | `SNOWFLAKE_USER` |
| Snowflake warehouse | `SNOWFLAKE_WH` |
| Pod image | `SNOWFLAKE_DBT_IMAGE` |

### `task_config` — cluster name from env

DBT DAGs read the cluster name from the `CLUSTER_NAME` environment variable instead of querying an Airflow Variable:

```python
bash_commands_config = "aws eks --region ap-southeast-1 update-kubeconfig --name " + os.getenv('CLUSTER_NAME')
```

### Runtime model selection

Add a `params` dict to the DAG so operators can pass specific model selectors when triggering manually:

```python
dag = DAG(
    ...,
    params={'models': 'model_name_1 model_name_2'}
)
```

Referenced in arguments as `{{{{ dag_run.conf['models'] }}}}`.

### Key differences from ETL DAGs at a glance

| | ETL DAG | DBT DAG |
|---|---|---|
| Failure callback | `task_fail_alert` | `task_fail_alert_dbt` |
| Bash arguments | Inline git clone + `python script.py` | `dbt_prefix` + `dbt build` + `dbt_failure_handling` |
| Snowflake user variable | `etl_snow_user` | `SNOWFLAKE_USER` |
| Snowflake warehouse variable | `etl_snow_wh` | `SNOWFLAKE_WH` |
| Pod image variable | `etl_image` | `SNOWFLAKE_DBT_IMAGE` |
| Cluster name source | `Variable.get("etl_env_prod")` → derived | `os.getenv('CLUSTER_NAME')` |
| Runtime params | Not used | `params={'models': '...'}` |
| AWS auth | Service account preferred | Service account (`etl_aws_service_account`) |

---

## 9. Architectural Design Decisions

### Full Load vs Incremental in the ETL Layer

The binding constraint is **source-side I/O and source DB health**, not pod memory. The executors already handle memory via chunked streaming (`chunksize=50000`), so peak pod memory is bounded regardless of table size.

The real bottleneck is:
- **Source DB query duration** — A full scan on a large PostgreSQL table locks more pages, spikes CPU/IO, and risks impacting live traffic.
- **Network / extraction time** — Moving all rows to S3 daily is the actual time cost.

Once data lands on S3, Snowflake ingestion via external stage (`COPY INTO`) is cheap and fast regardless of row count — it is not the bottleneck.

**Decision rule:** Use full load when the source table is small enough that a daily full scan is safe. Use incremental when table size makes daily full scans too slow or too risky for source health.

---

### Full Load vs Incremental in dbt Layer

Two reasons — both valid, different weights:

**Warehouse cost** — Full-refresh fact models reprocess the entire history on every run. Incremental models only process rows added since the last run, so compute scales with daily volume, not total history.

**Historical preservation / data quality** — More architecturally significant, especially for audit and reconciliation:

1. **Hard deletes in upstream** — A full-refresh fact silently drops rows that were deleted in source. An incremental fact retains them — correct behavior for audit (you want to know what was reported, not what currently exists in the OLTP).
2. **Intentional backdate exclusion** — Incremental ETL only pulls `WHERE DATE(date_column) >= ds`. Feeding this into an incremental dbt fact means the past is sealed. For financial reconciliation, today's corrections should not silently rewrite last month's figures.
3. **Pipeline SLA** — Incremental dbt runs complete faster, benefiting downstream dashboards and reports.

**Tradeoff:** Incremental + source hard deletes means ghost records accumulate. If deletes must propagate (GDPR, fraud removal), add a separate delete-handling step — typically a snapshot + tombstone strategy or a periodic full refresh of a small window.

---

### Full Load ETL + Incremental dbt Fact (Small Tables)

Valid and commonly used pattern.

The two layers have different responsibilities:
- **ETL raw layer** — mirrors current source state. Full load is fine for small tables; you get a clean daily snapshot without complexity.
- **dbt fact layer** — builds the analytical record. Incremental here means "once a row is committed to the fact, it stays" regardless of what happens upstream.

**The incremental fact layer is the one preserving history, not the raw layer. The raw layer doesn't need to.**

**The critical condition — incremental filter strategy in dbt:**

| dbt incremental strategy | Behavior |
|---|---|
| `created_at >= last_run` (append-only) | Only new rows enter the fact. Updates and deletes in source are invisible. Hard-delete protection works. |
| `updated_at >= last_run` + upsert on PK | Updated rows overwrite the fact record. Hard-delete protection exists but no full change history. |
| Snapshot (Type 2 SCD) | Full history of every change. A separate layer, not a standard incremental model. |

For audit/reconciliation, **append-only on `created_at`** is typically correct — journal entries and reconciliation records are immutable after creation.

**The key risk:** Since the raw table is truncated daily, you lose the ability to rebuild the fact from scratch using raw alone. A full historical backfill requires re-extracting from the source PostgreSQL directly.

**This pattern is appropriate when:**
- Fact rows are immutable once created (transactions, journal entries, audit events)
- You use append-only incremental in dbt (filter on `created_at`)
- You have a rebuild plan that does not depend on raw having history

If rows get meaningfully updated after creation and those updates need to be in the fact, consider incremental ETL or a dbt snapshot layer between raw and fact.

---

### Incremental Fact as Passive Audit / Anomaly Detection

An underrated benefit: the incremental fact layer acts as a **forensic baseline by design**. Because committed rows are never removed or overwritten, any upstream anomaly becomes detectable by comparing raw vs fact.

**What it catches:**

- **Hard deletes** — A row in yesterday's fact but missing from today's raw is a hard delete in source. Detectable by joining fact to raw on PK and filtering for nulls on the raw side.
- **Silent field mutations (no `updated_at` bump)** — If upstream code mutates a column (e.g. `amount`, `status`) without touching `updated_at`, raw silently reflects the wrong value while the fact holds the original. The divergence between `fact.amount` and `raw.amount` for the same PK is evidence of the bug. Without the incremental fact, that original value is gone forever.
- **Retroactive backdating** — A row inserted with a past `created_at` won't appear in the fact if you filter `created_at >= last_run`. The count discrepancy between raw and fact for that date partition is the signal.

Full-refresh models are always self-consistent with current raw state — they silently absorb upstream bugs with no trace. Incremental fact tables do not.

**Recommended convention:** Add a reconciliation check (dbt test or scheduled query) that flags `raw - fact` deltas above a threshold, turning the passive signal into an active alert. Document that intentional divergence between raw and fact is expected — otherwise it becomes a false alarm for the team.

---

### Decision Cheatsheet

**ETL layer:**

| | Full Load | Incremental |
|---|---|---|
| Table size | Small (daily full scan is safe) | Large (full scan hurts source DB) |
| Source DB impact | Higher | Lower |
| Complexity | Low | Higher (needs `date_key`, `join_key`) |

**dbt fact layer:**

| | Full Refresh | Incremental |
|---|---|---|
| Warehouse cost | Higher (reprocesses all history) | Lower (only new rows) |
| Hard delete propagation | Yes (mirrors source) | No (ghost records preserved) |
| Historical preservation | No | Yes |
| Forensic / audit value | None | High |
| Rebuild simplicity | Easy (just re-run) | Harder (depends on raw history) |
| Upstream bug detection | None | Yes (divergence = signal) |

---

## 10. Creating Tables and Running Backfills

Two utility DAGs handle table setup and one-off data loads. Both are triggered manually (no schedule) and take `domain` and `table_id` as params. `table_id` must match the `ID` column of the target row in `CONFIG.ETL.ETL_CONF_V2`.

### Creating a new Snowflake destination table

DAG: [dags/create_table_snowflake.py](dags/create_table_snowflake.py)

Trigger this when deploying a new ETL job before the first run. It reads the ETL_CONF_V2 row for the given `table_id`, introspects the source database schema, and creates the corresponding table in Snowflake.

| Param | Description |
|---|---|
| `domain` | Source domain (e.g. `evermos`, `everpro`, `erp`) — determines which credential variable (`etl_<domain>_secret`) to use |
| `table_id` | The `ID` value from the ETL_CONF_V2 config row |

Underlying script: `include/etl/create_table.py`

### Manual full load / backfill

DAG: [dags/manual_full_load_snowflake.py](dags/manual_full_load_snowflake.py)

Use this to seed a table on first deployment or to re-sync a table after schema changes or data loss. It performs a full truncate-insert using the config from the ETL_CONF_V2 row identified by `table_id`.

| Param | Description |
|---|---|
| `domain` | Source domain — same enum as `create_table_snowflake` |
| `table_id` | The `ID` value from the ETL_CONF_V2 config row |

Underlying script: `include/etl/fl_manual.py`

**Typical new-table workflow:**

1. Add the ETL_CONF_V2 config row (note the assigned `ID`).
2. Trigger `create_table_snowflake` with the `domain` and `table_id` to create the destination table.
3. Trigger `manual_full_load_snowflake` with the same params to seed the initial data.
4. Deploy and enable the scheduled DAG for ongoing loads.