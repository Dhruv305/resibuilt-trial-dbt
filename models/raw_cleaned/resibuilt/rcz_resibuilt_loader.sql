-- =============================================================================
-- MODEL: rcz_resibuilt_loader
-- =============================================================================
-- Single entry point for the Resibuilt RCZ domain.
--
-- The pre_hook invokes resibuilt_load_tables, which:
--   1. Reads var('resibuilt_table_config') from dbt_project.yml
--   2. Filters by var('resibuilt_tables') if provided (selective runs)
--   3. CREATE OR REPLACE TABLE {DBT_ENV_DFF}_RCZ.RESIBUILT.<table> for each
--   4. Writes a SUCCESS / FAILED audit row per table to
--      {DBT_ENV_DFF}_AUDIT.RESIBUILT.PIPELINE_AUDIT_LOG
--
-- The post_hook runs resibuilt_quality_checks on the freshly loaded tables.
--
-- Run patterns (all via Airflow / dbt Cloud, not typed by humans in prod):
--   -- full domain:
--   dbt run --select rcz_resibuilt_loader \
--       --vars '{"resibuilt_batch_id": "<id>"}'
--
--   -- selective (one or many tables):
--   dbt run --select rcz_resibuilt_loader \
--       --vars '{"resibuilt_batch_id": "<id>", "resibuilt_tables": ["PROPERTY","LEASE"]}'
-- =============================================================================

{{ config(
    materialized = 'view',
    alias        = 'RCZ_RESIBUILT_LOADER',
    tags         = ['resibuilt', 'loader'],
    pre_hook  = [
        "{{ resibuilt_load_tables(batch_id=var('resibuilt_batch_id'), table_filter=var('resibuilt_tables', none)) }}"
    ],
    post_hook = [
        "{{ resibuilt_quality_checks(batch_id=var('resibuilt_batch_id'), table_filter=var('resibuilt_tables', none)) }}"
    ]
) }}

SELECT
    '{{ var("resibuilt_batch_id") }}'::VARCHAR AS BATCH_ID,
    '{{ env_var("DBT_ENV_DFF", "DEV") }}'::VARCHAR AS ENV_PREFIX,
    CURRENT_TIMESTAMP()                        AS LOADER_COMPLETED_AT,
    '{{ invocation_id }}'::VARCHAR             AS DBT_INVOCATION_ID
