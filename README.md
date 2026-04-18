# Resibuilt Trial — Minimal dbt Project

Self-contained dbt project for testing the Resibuilt RCZ loader on a personal
Snowflake trial + dbt Cloud trial. This is a trimmed version of the pipeline
that lives in the org's `datalake-dbt-pipelines-main` repo.

## What it does

A single dbt model — `rcz_resibuilt_loader` — whose `pre_hook` reads
`resibuilt_table_config()` and performs `CREATE OR REPLACE TABLE
{DBT_ENV_DFF}_RCZ.RESIBUILT.<table>` for every enabled table, and whose
`post_hook` runs data-quality checks and updates the audit log.

## Project layout

```
dbt_project.yml              # project config (profile, model paths, vars)
packages.yml                 # empty - no external packages needed
profiles.yml                 # local CLI use only; dbt Cloud uses its own
macros/resibuilt/
    resibuilt_table_config.sql     # ALL tables live here (YAML in macro)
    resibuilt_helpers.sql          # env resolve, filter, audit helpers
    resibuilt_load_tables.sql      # the loader
    resibuilt_quality_checks.sql   # the post-load QC + FAILED audit updater
models/
    raw_cleaned/resibuilt/
        src_resibuilt.yml
        rcz_resibuilt_loader.sql   # single entry point
    audit/resibuilt/
        resibuilt_audit_summary.sql
```

## Running

### From dbt Cloud (primary)

Your dbt Cloud job's environment must set `DBT_ENV_DFF=DEV` (or `QA`, `PROD`).
The Airflow DAGs trigger the job with a `steps_override` equivalent to:

```bash
dbt run --select rcz_resibuilt_loader --vars '{"resibuilt_batch_id": "<id>"}'
```

### From CLI (local dev)

```bash
export SNOWFLAKE_ACCOUNT=<account>
export SNOWFLAKE_USER=DBT_CLOUD_USER
export SNOWFLAKE_PASSWORD=<pw>
export DBT_ENV_DFF=DEV

dbt deps
dbt debug
dbt run --select rcz_resibuilt_loader --vars '{"resibuilt_batch_id": "cli_test_001"}'

# selective run:
dbt run --select rcz_resibuilt_loader \
    --vars '{"resibuilt_batch_id": "cli_test_002", "resibuilt_tables": ["PROPERTY", "LEASE"]}'
```

## Adding a new table

Edit `macros/resibuilt/resibuilt_table_config.sql` and add a new entry under
`tables:`. No new models or macros required.
