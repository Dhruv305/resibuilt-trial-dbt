-- =============================================================================
-- MACRO: resibuilt_load_tables
-- =============================================================================
-- Config-driven loader for the Resibuilt domain. Reads resibuilt_table_config()
-- and loops over enabled tables, producing clean rows in:
--     {env}_RCZ.RESIBUILT.<table>
-- and one audit row per table in:
--     {env}_AUDIT.RESIBUILT.PIPELINE_AUDIT_LOG
--
-- Runtime overrides (passed via --vars from Airflow / dbt Cloud steps_override):
--   resibuilt_batch_id : current batch id (required for audit)
--   resibuilt_tables   : optional list of table names to restrict the run
--
-- Called as pre_hook of model rcz_resibuilt_loader. Also callable as:
--   dbt run-operation resibuilt_load_tables
--   dbt run-operation resibuilt_load_tables --args '{table_filter: ["PROPERTY"]}'
--
-- Load strategies
-- ---------------
--   full_refresh  : always CREATE OR REPLACE TABLE target AS SELECT ... FROM source
--   incremental   : if target table does not exist → initial full load via
--                   CREATE TABLE AS SELECT (no filter)
--                   if target exists               → INSERT INTO target
--                   SELECT ... FROM source
--                   WHERE <src_watermark_col> > MAX(<tgt_watermark_col> in target)
--
-- Notes
-- -----
--   * The watermark filter uses the actual business timestamp (e.g. UPDATED_DT)
--     rather than PIPELINE_INSERTED_AT, so re-running with no new source data
--     correctly produces 0 new rows while preserving previously loaded rows.
--   * Source and target watermark columns can have different names (via the
--     transforms). We map source_col → target_col to read MAX() from the
--     correct target column.
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
        {% set tbl_name      = tbl.table_name %}
        {% set load_strategy = tbl.get('load_strategy', 'full_refresh') %}
        {{ log("  -> loading " ~ tbl_name ~ " [" ~ load_strategy ~ "]", info=True) }}

        {# ── column SELECT list from transforms ────────────────────────────── #}
        {% set col_expressions = [] %}
        {% set src_to_tgt      = {} %}
        {% for t in tbl.transforms %}
            {% if t.get('derived') %}
                {% do col_expressions.append(t.expression ~ " AS " ~ t.target_col) %}
            {% else %}
                {% set src       = t.source_col %}
                {% set tgt       = t.target_col %}
                {% set cast_type = t.get('cast', '') %}
                {% set pii_mask  = t.get('pii_mask', '') %}

                {% do src_to_tgt.update({src: tgt}) %}

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

        {# pipeline metadata — appended to every target table.
           IMPORTANT: cast all string-literal columns to VARCHAR explicitly.
           Without an explicit cast, Snowflake infers the column width from the
           FIRST literal written by a CTAS (e.g. 'manual' -> VARCHAR(6)), and a
           later INSERT with a longer value (e.g. a 28-char sched_ batch_id)
           fails with Snowflake error 100078: "String '…' is too long and would
           be truncated". Casting to plain VARCHAR makes the column the default
           max-length VARCHAR(16777216). #}
        {% do col_expressions.append("CAST('RESIBUILT'        AS VARCHAR) AS SOURCE_SYSTEM") %}
        {% do col_expressions.append("CAST('" ~ tbl_name  ~ "' AS VARCHAR) AS SOURCE_TABLE_NAME") %}
        {% do col_expressions.append("CAST('" ~ batch_id ~ "' AS VARCHAR) AS BATCH_ID") %}
        {% do col_expressions.append("CURRENT_TIMESTAMP()                 AS PIPELINE_INSERTED_AT") %}
        {% do col_expressions.append("CURRENT_DATE()                      AS PIPELINE_LOAD_DATE") %}

        {# ── resolve target-side watermark column name ─────────────────────── #}
        {% set src_watermark = tbl.get('watermark_col') %}
        {% set tgt_watermark = src_to_tgt.get(src_watermark) if src_watermark else none %}

        {% if execute %}
            {# ── check target existence ────────────────────────────────────── #}
            {% set tbl_exists_q %}
                SELECT COUNT(*) FROM {{ env.tgt_database }}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{{ env.tgt_schema }}'
                  AND TABLE_NAME   = '{{ tbl_name }}'
            {% endset %}
            {% set tbl_exists = run_query(tbl_exists_q).columns[0].values()[0] | int %}

            {# ── decide whether we do a full rebuild or an incremental append ── #}
            {% set do_full_refresh = (load_strategy != 'incremental')
                                     or (tbl_exists == 0)
                                     or (tgt_watermark is none) %}

            {% set rows_before = 0 %}
            {% if not do_full_refresh %}
                {% set cnt_before_q = "SELECT COUNT(*) FROM " ~ env.tgt_database ~ "." ~ env.tgt_schema ~ "." ~ tbl_name %}
                {% set rows_before  = run_query(cnt_before_q).columns[0].values()[0] | int %}
            {% endif %}

            {% if do_full_refresh %}
                {% if load_strategy == 'incremental' and tbl_exists == 0 %}
                    {{ log("     target table missing -> full initial load", info=True) }}
                {% elif load_strategy == 'incremental' and tgt_watermark is none %}
                    {{ log("     WARN: incremental config has no resolvable target watermark column; falling back to full refresh", info=True) }}
                {% else %}
                    {{ log("     full_refresh: rebuilding target", info=True) }}
                {% endif %}

                {% set rcz_sql %}
                    CREATE OR REPLACE TABLE {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                    {% if tbl.get('rcz_cluster_by') %}
                        CLUSTER BY ({{ tbl.rcz_cluster_by }})
                    {% endif %}
                    AS
                    SELECT
                        {{ col_expressions | join(',\n                        ') }}
                    FROM {{ env.src_database }}.{{ env.src_schema }}.{{ tbl_name }}
                {% endset %}
                {% do run_query(rcz_sql) %}
            {% else %}
                {# incremental append from source business watermark column #}
                {% set last_wm_q %}
                    SELECT TO_VARCHAR(
                        COALESCE(MAX({{ tgt_watermark }}), '1900-01-01'::TIMESTAMP_NTZ)
                    )
                    FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                {% endset %}
                {% set last_wm = run_query(last_wm_q).columns[0].values()[0] %}
                {{ log("     incremental append where " ~ src_watermark ~ " > '" ~ last_wm ~ "'", info=True) }}

                {% set incr_sql %}
                    INSERT INTO {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                    SELECT
                        {{ col_expressions | join(',\n                        ') }}
                    FROM {{ env.src_database }}.{{ env.src_schema }}.{{ tbl_name }}
                    WHERE {{ src_watermark }} > '{{ last_wm }}'
                {% endset %}
                {% do run_query(incr_sql) %}
            {% endif %}

            {# ── compute row counts for audit + logging ───────────────────── #}
            {% set cnt_after_q = "SELECT COUNT(*) FROM " ~ env.tgt_database ~ "." ~ env.tgt_schema ~ "." ~ tbl_name %}
            {% set rows_after  = run_query(cnt_after_q).columns[0].values()[0] | int %}
            {% set rows_loaded = rows_after if do_full_refresh else (rows_after - rows_before) %}

            {{ resibuilt_write_audit_log(
                env.aud_database, env.aud_schema,
                tbl_name, batch_id,
                load_strategy, rows_loaded,
                'SUCCESS', ''
            ) }}
            {{ log("     rows loaded: " ~ rows_loaded ~ " (total in target: " ~ rows_after ~ ")", info=True) }}
        {% endif %}

    {% endfor %}

{% endmacro %}
