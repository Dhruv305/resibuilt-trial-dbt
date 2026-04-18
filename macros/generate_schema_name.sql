{#
  Override dbt's default generate_schema_name so that when a model specifies
  a custom schema (via +schema: in dbt_project.yml), dbt uses that schema
  LITERALLY — not "{target.schema}_{custom_schema}".

  Without this override, setting `+schema: RESIBUILT` against a target whose
  deployment credentials also point at schema RESIBUILT would produce the
  (incorrect) schema "RESIBUILT_RESIBUILT" and fail with:
      Schema 'DEV_RCZ.RESIBUILT_RESIBUILT' does not exist or not authorized.

  This mirrors the behaviour of the main org repo (datalake-dbt-pipelines-main).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
