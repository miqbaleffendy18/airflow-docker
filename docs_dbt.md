# dbt Patterns in corporate-dwh

This document explains recurring dbt patterns used across this project: dimension snapshots, SCD key integration in facts, partial refresh, incremental lookback windows, and incremental population CTEs.

---

## 1. Snapshot of dim tables (`snapshots/dim_dwh`)

Dimension tables are captured as [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots) so we retain history of attribute changes (SCD Type 2). Each snapshot file lives in `snapshots/dim_dwh/` and wraps a `SELECT` from a source model (e.g. a `dim_*` model or staging model) in a `{% snapshot %}` block.

Example — `snapshots/dim_dwh/dim_evermos_user.sql`:

```sql
{% snapshot dim_evermos_user %}
{{
    config(
        target_schema=target_schema_snapshot(),
        unique_key='USER_ID',
        strategy='check',
        check_cols = 'all',
        tags = ['dim_edna']
    )
}}

    SELECT
        user_id,
        brand_id,
        ...
    FROM {{ ref('evm_dim_reseller') }}
{% endsnapshot %}
```

Key points:

- **`target_schema=target_schema_snapshot()`** — a project macro (`macros/general/target_schema_snapshot.sql`) that resolves the destination schema based on the active `target.name` (e.g. `CORP_DIM` in prod, `<schema>_DIM` in dev, `CORP_SNAP`/`CORP_SNAP_DEV` for the snapshot-specific targets). This keeps snapshot output out of the regular build schema.
- **`strategy='check'` with `check_cols='all'`** — dbt compares every column of the incoming row to the last snapshotted row; if anything changed, it closes the old record (`dbt_valid_to`) and inserts a new one. This is used instead of `strategy='timestamp'` because most upstream tables don't have a fully reliable `updated_at` covering every trackable field.
- Every dim snapshot is tested for uniqueness on `(business_key, dbt_valid_to)` via `dbt_utils.unique_combination_of_columns` in `snapshots/dim_dwh/_dim_dwh_models.yml`, guaranteeing no overlapping "current" rows per key.
- dbt automatically adds `dbt_scd_id`, `dbt_valid_from`, `dbt_valid_to`, and `dbt_updated_at` columns to every snapshot. `dbt_scd_id` is the surrogate key for a specific version of a dimension row and is what fact tables join against (see §2).

---

## 2. Integration of dim SCD id in fact tables

Fact models join to snapshot dimensions to resolve a dimension row **as of the fact record**, then carry the dimension's `dbt_scd_id` (not the natural business key) as the fact's foreign key. This makes the fact row point at the exact historical version of the dimension that was active at load time.

Example — [fact_order_evermos.sql:212-214](../models/fact/fact_evermos/fact_order_evermos.sql#L212-L214):

```sql
-- DIM Key
u.dbt_scd_id AS dim_reseller_key,
b.dbt_scd_id AS dim_brand_key,
p.dbt_scd_id AS dim_product_key,
...
FROM evm_order o
JOIN {{ ref('evm_order_detail_evm') }} od ON od.order_id = o.order_id
LEFT JOIN {{ ref('dim_evermos_user') }} u ON od.user_id = u.user_id AND u.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_evermos_brand') }} b ON od.brand_id = b.brand_id AND b.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_evermos_product') }} p ON od.product_id = p.product_variant_id AND p.dbt_valid_to IS NULL
```

Key points:

- The join is always on the dimension's **natural/business key** (`user_id`, `brand_id`, `product_variant_id`) — never on `dbt_scd_id` directly, since the fact source doesn't know that surrogate id.
- **`dbt_valid_to IS NULL`** restricts the join to the *currently active* version of the dimension row at the time the fact is (re)built. This is a "current-state" join, not a true point-in-time-of-the-event join — if the dimension changes later, historical fact rows still point at the `dbt_scd_id` that was current when that fact row was last written, and won't automatically repoint unless the fact row is reprocessed.
- The resulting `dim_*_key` column (aliased as `dim_reseller_key`, `dim_brand_key`, `dim_product_key`) is the value stored on the fact table and used to join back to the dimension in downstream marts/BI.
- Any fact model that needs a dimension attribute follows this same shape: `LEFT JOIN {{ ref('dim_xxx') }} alias ON <business_key match> AND alias.dbt_valid_to IS NULL`.
- **This mechanism only preserves history correctly when the fact model is `materialized='incremental'`.** Because the join always resolves to whatever dimension version is *currently* active (`dbt_valid_to IS NULL`), a full-refresh/table run re-evaluates every historical fact row against today's current dimension state — overwriting every `dim_*_key` with the dimension's latest `dbt_scd_id`, regardless of what was active when that fact event actually happened. On an incremental run, only new/changed fact rows are (re)computed, so already-written historical rows keep the `dbt_scd_id` that was current at the time they were originally loaded, and the SCD history is preserved. In other words: incremental processing is what turns this into a "point-in-time at load" join instead of a "current-state, applied retroactively to all history" join.

---

## 3. Partial refresh mechanism

Some fact tables support a **partial refresh** mode: instead of processing every row every run, only rows whose dependent value(s) actually *changed* compared to what's already in the target table are reprocessed. This is controlled by a dbt var and applied as an extra `WHERE` filter that self-joins to the existing table.

Example — [fact_finance_ap.sql:1049-1070](../models/fact/fact_evermos/fact_finance_ap.sql#L1049-L1070):

```sql
{% set compare_column = var("compare_column", none) %}
{% set is_sales_channel_feature = var("is_sales_channel_feature", True) %}

{% if is_partial_refresh() %}
LEFT JOIN {{ this }} AS previous ON a.finance_ap_id = previous.finance_ap_id
WHERE (
    previous.sales_channel_code IS DISTINCT FROM
    {% if is_sales_channel_feature %}
        a.sales_channel_code_feature
    {% else %}
        b.sales_channel_code
    {% endif %}

    {% if compare_column is not none %}
    OR (
        previous.{{ compare_column }} IS DISTINCT FROM a.{{ compare_column }}
        AND previous.sales_channel_code IS NOT NULL
    )
    {% endif %}
)
AND a.finance_transaction_date::DATE >= '2026-04-01' -- Migration date to sales channel feature
{% endif %}
```

And the supporting macro, `macros/general/partial_refresh.sql`:

```sql
{% macro is_partial_refresh() %}
    {{ return(var('partial_refresh', false)) }}
{% endmacro %}
```

Key points:

- `is_partial_refresh()` just reads the `partial_refresh` dbt var, defaulting to `false`. It's invoked at run time, e.g.:
  ```
  dbt run -s fact_finance_ap --vars '{"partial_refresh": true, "compare_column": "some_column"}'
  ```
- When enabled, the model **self-joins against `{{ this }}`** (the model's own existing target table, aliased `previous`) to compare the newly computed value against what's currently stored.
- The core comparison uses `IS DISTINCT FROM`, which (unlike `!=`) correctly treats `NULL` vs `NULL` as *not* different and `NULL` vs a value as different — important because these are frequently nullable columns.
- `compare_column` is an optional extra column to compare (passed via `--vars`), letting the same partial-refresh gate be reused for backfilling different columns without duplicating the model.
- The trailing date filter (`a.finance_transaction_date::DATE >= '2026-04-01'`) scopes the partial refresh to only the period relevant to the migration that motivated this pattern — rows before that date are left untouched.
- This pattern is distinct from a normal `is_incremental()` filter: it doesn't limit which rows are *read*, it limits which of the freshly computed rows are actually written back, based on whether the value differs from what's already there. It's typically used for a targeted backfill/correction run rather than the model's regular daily execution.

---

## 4. Incremental with 1-day offset lookback

Standard incremental models filter upstream sources by `meta_updated_at`/`updated_at` compared against the max `etl_date` already loaded — but with a **1-day lookback buffer** to protect against late-arriving or slightly out-of-order data near the incremental boundary.

Example — [fact_order_evermos.sql:32,36,40](../models/fact/fact_evermos/fact_order_evermos.sql#L32-L40):

```sql
{% if is_incremental() %}
control_cte AS (
    SELECT order_id
    FROM {{ ref('evm_evm_order') }}
    WHERE meta_updated_at >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})
    UNION
    SELECT order_id
    FROM {{ ref('evm_evm_order_receipt') }}
    WHERE meta_updated_at >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})
    UNION
    SELECT order_id
    FROM {{ ref('evm_evm_order_detail') }}
    WHERE meta_updated_at >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})
),
{% endif %}
```

Key points:

- `(SELECT MAX(etl_date) - INTERVAL '1 day' FROM {{ this }})::DATE` computes the incremental cutoff as *one day before* the latest `etl_date` already in the target table, instead of using the raw max. This re-scans the last day of already-loaded data on every run.
- Why: `etl_date` is stamped at load time, not event time, so a row could arrive/update slightly after the previous run's watermark but still logically belong to an already-processed batch. The 1-day buffer re-pulls that window so those late updates aren't permanently missed, at the cost of reprocessing a small amount of already-current data (cheap, since `delete+insert` + a narrow key filter keeps it idempotent).
- The same offset is applied consistently across **every source table** feeding the incremental filter (`evm_evm_order`, `evm_evm_order_receipt`, `evm_evm_order_detail`) so the resulting `order_id` set is a complete union of anything that could have changed in that window, from any of the tables that make up the fact grain.
- The `control_cte` result (a set of natural keys, e.g. `order_id`) is then used to scope every downstream CTE in the model (`WHERE order_id IN (SELECT order_id FROM control_cte)`), ensuring all joined tables are filtered consistently rather than each independently applying its own date filter.
- This pattern pairs with `incremental_strategy='delete+insert'` on `unique_key`, so reprocessing the lookback window safely replaces existing rows rather than duplicating them.

---

## 5. Populate incremental "orders" target (incremental population CTE)

A variant of the lookback pattern above, used when the fact's grain can change **indirectly** — e.g. through a related/child entity — not just via its own `updated_at`. The population CTE unions together (a) directly-updated rows and (b) rows whose *related* entity was updated, so both trigger a reprocess of the same target rows.

Example — [fact_everpro_invoice_detail.sql:14-28](../models/fact/fact_everpro/fact_everpro_invoice_detail.sql#L14-L28):

```sql
WITH
{% if is_incremental() %}
    invoice_population AS (
        SELECT invoice_transaction_id
        FROM {{ ref('popaket_user_invoices') }}
        WHERE meta_updated_at::DATE >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})
        UNION
        SELECT DISTINCT i.invoice_transaction_id
        FROM {{ ref('popaket_user_invoices') }} i
        LEFT JOIN {{ ref('popaket_user_subscription_invoice_mapping') }} sim
            ON sim.invoice_entity_id = i.invoice_transaction_id
        LEFT JOIN {{ ref('popaket_user_subscriptions') }} s
            ON s.subscription_id = sim.subscription_entity_id
        WHERE s.updated_at::DATE >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})
    ),
{% endif %}

invoice AS (
    SELECT *
    FROM {{ ref('popaket_user_invoices') }}
    {% if is_incremental() %}
        WHERE invoice_transaction_id IN (SELECT invoice_transaction_id FROM invoice_population)
    {% endif %}
),
```

Key points:

- Branch 1 (`SELECT invoice_transaction_id FROM popaket_user_invoices WHERE meta_updated_at >= ...`) is the direct case: the invoice itself was updated — same 1-day-offset lookback as §4.
- Branch 2 walks from `popaket_user_subscriptions` (the entity that actually changed) through the `popaket_user_subscription_invoice_mapping` bridge table back to `invoice_transaction_id`. This catches cases where an invoice's *subscription* changed (e.g. a plan/status update) even though the invoice row itself has no new `meta_updated_at`, which would otherwise cause that invoice to be silently skipped.
- Both branches are combined with `UNION` into a single `invoice_population` CTE holding the target's unique key (`invoice_transaction_id`) — the same key as the model's `unique_key` config — and that CTE is the single gate every downstream CTE filters against (`WHERE invoice_transaction_id IN (SELECT invoice_transaction_id FROM invoice_population)`).
- General shape to reuse for a new fact: for every upstream entity that can affect a fact row *without directly touching the fact's own base table*, add a branch that traces from that entity's `updated_at` back to the fact's unique key via its join/bridge path, then `UNION` it into the population CTE.

---

## 6. Every fact table requires an `etl_date` column

Every fact model must expose an `etl_date` column (typically `(CURRENT_TIMESTAMP + INTERVAL '7 hours')::TIMESTAMP AS etl_date`, stamped at build time — see §4). It serves two purposes:

1. **It's the model's own incremental watermark** — every `is_incremental()`/`run_incremental()` filter in this doc (§4, §5) reads `MAX(etl_date) FROM {{ this }}` to know where the last run left off.
2. **It's the reference watermark for downstream fact tables that consume this fact as a source.** When another fact model reads from this one, it applies the same 1-day-offset lookback, but against *this* table's `etl_date` — not its own.

Example — `fact_finance_ap.sql` reads from `fact_order_evermos` and `fact_order_berikhtiar`, filtering each by the upstream fact's own `etl_date`:

[fact_finance_ap.sql:340-349](../models/fact/fact_evermos/fact_finance_ap.sql#L340-L349) (source: `fact_order_evermos`):

```sql
FROM {{ ref('fact_order_evermos') }}
WHERE 1=1
{% if run_incremental() %}
    AND (
        etl_date::DATE >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})
        OR order_detail_id::VARCHAR IN (
            SELECT reference_id FROM {{ ref('fact_order_stream') }}
            WHERE reference_source = 'EVM_ORDER_DETAIL_ID'
            AND meta_updated_at::DATE >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})
            AND meta_deleted_at IS NULL
        )
        OR waybill IN (SELECT waybill FROM evp_shipping_all)
    )
```

[fact_finance_ap.sql:424-433](../models/fact/fact_evermos/fact_finance_ap.sql#L424-L433) (source: `fact_order_berikhtiar`) applies the identical shape.

Key points:

- `etl_date::DATE >= (SELECT (MAX(etl_date) - INTERVAL '1 day')::DATE FROM {{ this }})` — the left-hand `etl_date` belongs to the **source** table (`fact_order_evermos`/`fact_order_berikhtiar`), while the subquery's `MAX(etl_date) FROM {{ this }}` is `fact_finance_ap`'s own watermark. Because every fact table carries `etl_date`, any downstream fact can apply the same lookback-filter pattern against any upstream fact, regardless of what that upstream fact's own incremental logic looks like internally.
- This is why `etl_date` can't be treated as an optional/cosmetic audit column — omitting it from a fact model breaks incremental filtering both for that model itself (§4/§5) and for any downstream fact that needs to consume it incrementally (this section).
- The `OR order_detail_id::VARCHAR IN (...)` / `OR waybill IN (...)` branches layered on top follow the same "population CTE" idea as §5 — catching rows that changed indirectly (via `fact_order_stream` or shipping data) even when the source fact's own `etl_date` wasn't refreshed.

---

## Summary table

| # | Pattern | Where | Purpose |
|---|---------|-------|---------|
| 1 | Snapshot of dim tables | `snapshots/dim_dwh/*.sql` | Track SCD Type 2 history of dimension attributes |
| 2 | Dim SCD id in facts | `dim_*.dbt_scd_id AS dim_*_key` joined on `dbt_valid_to IS NULL` | Store the exact dimension version a fact row resolved to |
| 3 | Partial refresh | `is_partial_refresh()` + self-join to `{{ this }}` | Selectively reprocess only rows whose value changed, gated by a var |
| 4 | Incremental w/ 1-day offset | `MAX(etl_date) - INTERVAL '1 day'` filter on every source | Re-scan a 1-day buffer to catch late-arriving/updated rows |
| 5 | Incremental population CTE | `*_population` CTE unioning direct + related-entity changes | Catch fact-grain changes triggered by a related/child entity, not just the base table |
| 6 | Required `etl_date` column | Every fact model's `SELECT` list | Own incremental watermark, and the watermark downstream facts filter on when consuming this fact as a source |
