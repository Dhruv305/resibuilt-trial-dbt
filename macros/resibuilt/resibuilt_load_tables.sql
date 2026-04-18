-- =============================================================================
-- MACRO: resibuilt_load_tables
-- =============================================================================
-- Config-driven loader for the Resibuilt domain. Reads var('resibuilt_table_config')
-- and loops over enabled tables, producing:
--   CREATE OR REPLACE TABLE {env}_RCZ.RESIBUILT.<table>  (clean rows)
--   one audit row per table in {env}_AUDIT.RESIBUILT.PIPELINE_AUDIT_LOG
--
-- Runtime overrides (passed via --vars from Airflow / dbt Cloud steps_override):
--   resibuilt_batch_id : current batch id (required for audit)
--   resibuilt_tables   : optional list of table names to restrict the run
--
-- Called as pre_hook of model rcz_resibuilt_loader. Also callable as:
--   dbt run-operation resibuilt_load_tables
--   dbt run-operation resibuilt_load_tables --args '{table_filter: ["PROPERTY"]}'
-- =============================================================================

{% macro resibuilt_load_tables(batch_id=none, table_filter=none) %}

    {% set env      = resibuilt_resolve_env() %}
    {% set cfg      = resibuilt_table_config() %}
    {% set batch_id = batch_id or var('resibuilt_batch_id', '1') %}
    {% set filter   = table_filter if table_filter is not none else var('resibuilt_tables', none) %}

    {% set tables_to_run = resibuilt_filter_tables(cfg.tables, filter) %}

    {{ log("=== resibuilt_load_tables: batch_id=" ~ batch_id
           ~ " env=" ~ env.env_prefix
           ~ " tables=" ~ (tables_to_run | length), info=True) }}

    {{ resibuilt_ensure_audit_table(env.aud_database, env.aud_schema) }}

    {% for tbl in tables_to_run %}
        {% set tbl_name = tbl.table_name %}
        {{ log("  -> loading " ~ tbl_name, info=True) }}

        {# ── column SELECT list from transforms ────────────────────────────── #}
        {% set col_expressions = [] %}
        {% for t in tbl.transforms %}
            {% if t.get('derived') %}
                {% do col_expressions.append(t.expression ~ " AS " ~ t.target_col) %}
            {% else %}
                {% set src       = t.source_col %}
                {% set tgt       = t.target_col %}
                {% set cast_type = t.get('cast', '') %}
                {% set pii_mask  = t.get('pii_mask', '') %}

                {% if pii_mask == 'SHA256' %}
                    {% if cast_type %}
                        {% do col_expressions.append(
                            "SHA2(CAST(" ~ src ~ " AS " ~ cast_type ~ "), 256) AS " ~ tgt
                        ) %}
                    {% else %}
                        {% do col_expressions.append("SHA2(" ~ src ~ ", 256) AS " ~ tgt) %}
                    {% endif %}
                {% elif cast_type %}
                    {% do col_expressions.append(
                        "CAST(" ~ src ~ " AS " ~ cast_type ~ ") AS " ~ tgt
                    ) %}
                {% else %}
                    {% do col_expressions.append(src ~ " AS " ~ tgt) %}
                {% endif %}
            {% endif %}
        {% endfor %}

        {# pipeline metadata — appended to every target table #}
        {% do col_expressions.append("'RESIBUILT'           AS SOURCE_SYSTEM") %}
        {% do col_expressions.append("'" ~ tbl_name ~ "'    AS SOURCE_TABLE_NAME") %}
        {% do col_expressions.append("'" ~ batch_id ~ "'    AS BATCH_ID") %}
        {% do col_expressions.append("CURRENT_TIMESTAMP()   AS PIPELINE_INSERTED_AT") %}
        {% do col_expressions.append("CURRENT_DATE()        AS PIPELINE_LOAD_DATE") %}

        {# ── incremental watermark filter ──────────────────────────────────── #}
        {% set incr_filter = '' %}
        {% if tbl.load_strategy == 'incremental' and tbl.get('watermark_col') %}
            {% if execute %}
                {% set tbl_exists_q %}
                    SELECT COUNT(*) FROM {{ env.tgt_database }}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{{ env.tgt_schema }}'
                      AND TABLE_NAME   = '{{ tbl_name }}'
                {% endset %}
                {% set tbl_exists = run_query(tbl_exists_q).columns[0].values()[0] %}
                {% if tbl_exists > 0 %}
                    {% set last_wm_q %}
                        SELECT COALESCE(MAX(PIPELINE_INSERTED_AT), '1900-01-01'::TIMESTAMP_NTZ)
                        FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                    {% endset %}
                    {% set last_wm = run_query(last_wm_q).columns[0].values()[0] %}
                    {% set incr_filter = "WHERE " ~ tbl.watermark_col ~ " > '" ~ last_wm ~ "'" %}
                {% else %}
                    {{ log("     target table missing -> full initial load", info=True) }}
                {% endif %}
            {% endif %}
        {% endif %}

        {% if execute %}
            {# ── write target table ─────────────────────────────────────────── #}
            {% set rcz_sql %}
                CREATE OR REPLACE TABLE {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                {% if tbl.get('rcz_cluster_by') %}
                    CLUSTER BY ({{ tbl.rcz_cluster_by }})
                {% endif %}
                AS
                SELECT
                    {{ col_expressions | join(',\n                    ') }}
                FROM {{ env.src_database }}.{{ env.src_schema }}.{{ tbl_name }}
                {{ incr_filter }}
            {% endset %}

            {% set row_count = 0 %}
            {% set load_status = 'SUCCESS' %}
            {% set err_msg = '' %}
            {% do run_query(rcz_sql) %}
            {% set cnt_q = "SELECT COUNT(*) FROM " ~ env.tgt_database ~ "." ~ env.tgt_schema ~ "." ~ tbl_name %}
            {% set row_count = run_query(cnt_q).columns[0].values()[0] %}

            {{ resibuilt_write_audit_log(
                env.aud_database, env.aud_schema,
                tbl_name, batch_id,
                tbl.load_strategy, row_count,
                load_status, err_msg
            ) }}
            {{ log("     rows loaded: " ~ row_count, info=True) }}
        {% endif %}

    {% endfor %}

{% endmacro %}
