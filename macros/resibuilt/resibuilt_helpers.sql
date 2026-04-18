-- =============================================================================
-- MACRO: resibuilt_resolve_env
-- Returns dict of resolved database names for the current DBT_ENV_DFF.
-- =============================================================================
{% macro resibuilt_resolve_env() %}
    {% set prefix = env_var('DBT_ENV_DFF', 'DEV') %}
    {% set cfg    = resibuilt_table_config() %}
    {% set out = {
        'env_prefix':    prefix,
        'src_database':  prefix ~ '_RZ',
        'tgt_database':  prefix ~ '_RCZ',
        'aud_database':  prefix ~ '_AUDIT',
        'src_schema':    cfg.source_schema,
        'tgt_schema':    cfg.target_schema,
        'aud_schema':    cfg.audit_schema
    } %}
    {{ return(out) }}
{% endmacro %}


-- =============================================================================
-- MACRO: resibuilt_filter_tables
-- Given the full tables list from table_config and an optional filter list,
-- returns only the enabled tables whose names match the filter (case-insensitive).
-- A None / empty filter returns all enabled tables.
-- =============================================================================
{% macro resibuilt_filter_tables(all_tables, table_filter=none) %}
    {% set selected = [] %}
    {% set filter_upper = [] %}
    {% if table_filter is not none and table_filter | length > 0 %}
        {% for t in table_filter %}
            {% do filter_upper.append(t | string | upper) %}
        {% endfor %}
    {% endif %}

    {% for tbl in all_tables %}
        {% if tbl.enabled %}
            {% if filter_upper | length == 0 or (tbl.table_name | upper) in filter_upper %}
                {% do selected.append(tbl) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {{ return(selected) }}
{% endmacro %}


-- =============================================================================
-- MACRO: resibuilt_ensure_audit_table
-- Creates PIPELINE_AUDIT_LOG in {env}_AUDIT.RESIBUILT if missing.
-- =============================================================================
{% macro resibuilt_ensure_audit_table(aud_db, aud_sch) %}
    {% if execute %}
        {% set ddl %}
            CREATE TABLE IF NOT EXISTS {{ aud_db }}.{{ aud_sch }}.PIPELINE_AUDIT_LOG (
                AUDIT_ID            VARCHAR(50)   DEFAULT UUID_STRING(),
                BATCH_ID            VARCHAR(50)   NOT NULL,
                TABLE_NAME          VARCHAR(200)  NOT NULL,
                LOAD_STRATEGY       VARCHAR(20),
                ROWS_LOADED         INTEGER,
                PIPELINE_STATUS     VARCHAR(20)   DEFAULT 'SUCCESS',
                ERROR_MESSAGE       VARCHAR(4000),
                STARTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                COMPLETED_AT        TIMESTAMP_NTZ,
                DURATION_SECONDS    NUMBER(10,2),
                LOADED_BY           VARCHAR(100)  DEFAULT CURRENT_USER(),
                DBT_INVOCATION_ID   VARCHAR(100)
            )
        {% endset %}
        {% do run_query(ddl) %}
    {% endif %}
{% endmacro %}


-- =============================================================================
-- MACRO: resibuilt_write_audit_log
-- Inserts one audit record per table per batch. Status: SUCCESS or FAILED.
-- =============================================================================
{% macro resibuilt_write_audit_log(aud_db, aud_sch, tbl_name, batch_id, strategy, rows_loaded, status='SUCCESS', error_message='') %}
    {% if execute %}
        {% set safe_error = (error_message | default('', true)) | replace("'", "''") %}
        {% set insert_sql %}
            INSERT INTO {{ aud_db }}.{{ aud_sch }}.PIPELINE_AUDIT_LOG (
                BATCH_ID,
                TABLE_NAME,
                LOAD_STRATEGY,
                ROWS_LOADED,
                PIPELINE_STATUS,
                ERROR_MESSAGE,
                COMPLETED_AT,
                DBT_INVOCATION_ID
            ) VALUES (
                '{{ batch_id }}',
                '{{ tbl_name }}',
                '{{ strategy }}',
                {{ rows_loaded | default(0, true) }},
                '{{ status }}',
                {% if safe_error %}'{{ safe_error }}'{% else %}NULL{% endif %},
                CURRENT_TIMESTAMP(),
                '{{ invocation_id }}'
            )
        {% endset %}
        {% do run_query(insert_sql) %}
    {% endif %}
{% endmacro %}
