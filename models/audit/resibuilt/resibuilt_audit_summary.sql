-- =============================================================================
-- MODEL: resibuilt_audit_summary
-- =============================================================================
-- Queryable view over the Resibuilt pipeline audit log. Used by:
--   * dashboards / alerting
--   * the resibuilt_manual_ops DAG to compute which tables failed in the
--     last batch so they can be re-run selectively
-- =============================================================================

{{ config(
    materialized = 'view',
    alias        = 'RESIBUILT_AUDIT_SUMMARY',
    tags         = ['resibuilt', 'audit', 'monitoring']
) }}

WITH base AS (
    SELECT
        AUDIT_ID,
        BATCH_ID,
        TABLE_NAME,
        LOAD_STRATEGY,
        ROWS_LOADED,
        PIPELINE_STATUS,
        ERROR_MESSAGE,
        STARTED_AT,
        COMPLETED_AT,
        DURATION_SECONDS,
        LOADED_BY,
        DBT_INVOCATION_ID
    FROM {{ env_var('DBT_ENV_DFF', 'DEV') }}_AUDIT.RESIBUILT.PIPELINE_AUDIT_LOG
)

SELECT
    *,
    CASE
        WHEN PIPELINE_STATUS = 'FAILED' THEN 'FAILED'
        WHEN ROWS_LOADED = 0            THEN 'EMPTY_LOAD'
        ELSE                                 'HEALTHY'
    END                                  AS HEALTH_STATUS,
    DATE(STARTED_AT)                     AS LOAD_DATE,
    DATE_TRUNC('hour', STARTED_AT)       AS LOAD_HOUR,
    RANK() OVER (
        PARTITION BY TABLE_NAME
        ORDER BY STARTED_AT DESC
    )                                    AS RECENCY_RANK
FROM base
