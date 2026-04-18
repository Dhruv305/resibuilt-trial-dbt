-- =============================================================================
-- MACRO: resibuilt_quality_checks
-- =============================================================================
-- Post-load data quality assertions driven by quality_checks in table_config.
-- Runs as post_hook of rcz_resibuilt_loader OR standalone:
--   dbt run-operation resibuilt_quality_checks
--   dbt run-operation resibuilt_quality_checks --args '{table_filter: ["PROPERTY"]}'
--
-- Severities:
--   error -> fail the run after the loop (all errors surfaced together)
--   warn  -> log only
--
-- Supported check types:
--   not_null, unique, range, regex, accepted_values, referential
-- =============================================================================

{% macro resibuilt_quality_checks(batch_id=none, table_filter=none) %}

    {% set env     = resibuilt_resolve_env() %}
    {% set cfg     = resibuilt_table_config() %}
    {% set batch_id = batch_id or var('resibuilt_batch_id', '1') %}
    {% set filter  = table_filter if table_filter is not none else var('resibuilt_tables', none) %}
    {% set tables_to_run = resibuilt_filter_tables(cfg.tables, filter) %}

    {% set error_failures = [] %}
    {% set failed_tables  = [] %}  {# distinct set of table names w/ error-severity failures #}

    {% for tbl in tables_to_run %}
        {% set tbl_name = tbl.table_name %}
        {{ log("-- QC: " ~ tbl_name ~ " --", info=True) }}

        {% for check in tbl.get('quality_checks', []) %}
            {% set check_type = check.type %}
            {% set severity   = check.get('severity', 'warn') %}
            {% set fail_count = 0 %}
            {% set check_sql  = '' %}
            {% set detail     = '' %}

            {% if check_type == 'not_null' %}
                {% for col in check.columns %}
                    {% set check_sql %}
                        SELECT COUNT(*) FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                        WHERE {{ col }} IS NULL
                    {% endset %}
                    {% if execute %}
                        {% set fail_count = run_query(check_sql).columns[0].values()[0] %}
                        {{ _resibuilt_log_qc(tbl_name, 'not_null', col, fail_count, severity) }}
                        {% if fail_count > 0 and severity == 'error' %}
                            {% do error_failures.append(tbl_name ~ ":not_null:" ~ col) %}
                            {% if tbl_name not in failed_tables %}{% do failed_tables.append(tbl_name) %}{% endif %}
                        {% endif %}
                    {% endif %}
                {% endfor %}

            {% elif check_type == 'unique' %}
                {% set pk = check.columns | join(', ') %}
                {% set check_sql %}
                    SELECT COUNT(*) FROM (
                        SELECT {{ pk }}, COUNT(*) AS cnt
                        FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                        GROUP BY {{ pk }}
                        HAVING cnt > 1
                    )
                {% endset %}
                {% if execute %}
                    {% set fail_count = run_query(check_sql).columns[0].values()[0] %}
                    {{ _resibuilt_log_qc(tbl_name, 'unique', pk, fail_count, severity) }}
                    {% if fail_count > 0 and severity == 'error' %}
                        {% do error_failures.append(tbl_name ~ ":unique:" ~ pk) %}
                        {% if tbl_name not in failed_tables %}{% do failed_tables.append(tbl_name) %}{% endif %}
                    {% endif %}
                {% endif %}

            {% elif check_type == 'range' %}
                {% set col = check.column %}
                {% set min_val = check.get('min', none) %}
                {% set max_val = check.get('max', none) %}
                {% set parts = [] %}
                {% if min_val is not none %}{% do parts.append(col ~ ' < ' ~ min_val) %}{% endif %}
                {% if max_val is not none %}{% do parts.append(col ~ ' > ' ~ max_val) %}{% endif %}
                {% set check_sql %}
                    SELECT COUNT(*) FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                    WHERE {{ col }} IS NOT NULL AND ({{ parts | join(' OR ') }})
                {% endset %}
                {% if execute and parts | length > 0 %}
                    {% set fail_count = run_query(check_sql).columns[0].values()[0] %}
                    {% set detail = col ~ " [" ~ (min_val|string) ~ "," ~ (max_val|string) ~ "]" %}
                    {{ _resibuilt_log_qc(tbl_name, 'range', detail, fail_count, severity) }}
                    {% if fail_count > 0 and severity == 'error' %}
                        {% do error_failures.append(tbl_name ~ ":range:" ~ col) %}
                        {% if tbl_name not in failed_tables %}{% do failed_tables.append(tbl_name) %}{% endif %}
                    {% endif %}
                {% endif %}

            {% elif check_type == 'regex' %}
                {% set col = check.column %}
                {% set pattern = check.pattern %}
                {% set check_sql %}
                    SELECT COUNT(*) FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                    WHERE {{ col }} IS NOT NULL AND NOT REGEXP_LIKE({{ col }}, '{{ pattern }}')
                {% endset %}
                {% if execute %}
                    {% set fail_count = run_query(check_sql).columns[0].values()[0] %}
                    {{ _resibuilt_log_qc(tbl_name, 'regex', col, fail_count, severity) }}
                    {% if fail_count > 0 and severity == 'error' %}
                        {% do error_failures.append(tbl_name ~ ":regex:" ~ col) %}
                        {% if tbl_name not in failed_tables %}{% do failed_tables.append(tbl_name) %}{% endif %}
                    {% endif %}
                {% endif %}

            {% elif check_type == 'accepted_values' %}
                {% set col = check.column %}
                {% set vals = check['values'] | map('string') | list %}
                {% set quoted = [] %}
                {% for v in vals %}{% do quoted.append("'" ~ v ~ "'") %}{% endfor %}
                {% set in_list = quoted | join(', ') %}
                {% set check_sql %}
                    SELECT COUNT(*) FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }}
                    WHERE {{ col }} IS NOT NULL AND {{ col }}::VARCHAR NOT IN ({{ in_list }})
                {% endset %}
                {% if execute %}
                    {% set fail_count = run_query(check_sql).columns[0].values()[0] %}
                    {{ _resibuilt_log_qc(tbl_name, 'accepted_values', col, fail_count, severity) }}
                    {% if fail_count > 0 and severity == 'error' %}
                        {% do error_failures.append(tbl_name ~ ":accepted_values:" ~ col) %}
                        {% if tbl_name not in failed_tables %}{% do failed_tables.append(tbl_name) %}{% endif %}
                    {% endif %}
                {% endif %}

            {% elif check_type == 'referential' %}
                {% set col = check.column %}
                {% set ref_tbl = check.ref_table %}
                {% set ref_col = check.ref_column %}
                {% set check_sql %}
                    SELECT COUNT(*)
                    FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ tbl_name }} child
                    WHERE child.{{ col }} IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {{ env.tgt_database }}.{{ env.tgt_schema }}.{{ ref_tbl }} parent
                          WHERE parent.{{ ref_col }} = child.{{ col }}
                      )
                {% endset %}
                {% if execute %}
                    {% set fail_count = run_query(check_sql).columns[0].values()[0] %}
                    {% set detail = col ~ " -> " ~ ref_tbl ~ "." ~ ref_col %}
                    {{ _resibuilt_log_qc(tbl_name, 'referential', detail, fail_count, severity) }}
                    {% if fail_count > 0 and severity == 'error' %}
                        {% do error_failures.append(tbl_name ~ ":referential:" ~ col) %}
                        {% if tbl_name not in failed_tables %}{% do failed_tables.append(tbl_name) %}{% endif %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {# ─────────────────────────────────────────────────────────────────────
       If any error-severity QC failures were found, mark the CURRENT batch's
       audit rows for those tables as FAILED so retry_failed mode can pick
       them up on the next run, then raise to fail the dbt run.
       ──────────────────────────────────────────────────────────────────── #}
    {% if execute and error_failures | length > 0 %}
        {% set err_summary = error_failures | join(', ') %}
        {% set safe_err    = err_summary | replace("'", "''") %}

        {% for t in failed_tables %}
            {% set update_sql %}
                UPDATE {{ env.aud_database }}.{{ env.aud_schema }}.PIPELINE_AUDIT_LOG
                SET PIPELINE_STATUS = 'FAILED',
                    ERROR_MESSAGE   = 'QC: {{ safe_err }}'
                WHERE BATCH_ID   = '{{ batch_id }}'
                  AND TABLE_NAME = '{{ t }}'
            {% endset %}
            {% do run_query(update_sql) %}
        {% endfor %}

        {{ exceptions.raise_compiler_error(
            "Resibuilt quality checks FAILED (error severity): " ~ err_summary
        ) }}
    {% endif %}

{% endmacro %}


{% macro _resibuilt_log_qc(tbl, check_type, detail, fail_count, severity) %}
    {% set status = "OK" if fail_count == 0 else ("FAIL" if severity == 'error' else "WARN") %}
    {{ log("   [" ~ status ~ "] " ~ tbl ~ " | " ~ check_type ~ " | " ~ detail ~ " | fails=" ~ fail_count, info=True) }}
{% endmacro %}
