-- =============================================================================
-- MACRO: resibuilt_table_config
-- =============================================================================
-- Single source of truth for the Resibuilt table pipeline configuration.
--
-- Why this lives in a macro (not a plain .yml file):
--   dbt's Jinja sandbox cannot read arbitrary files from disk at runtime
--   (no open(), no filesystem access, including in dbt Cloud). The workaround
--   is to keep the YAML *inside* a macro and parse it via fromyaml(). The
--   end result is identical to an external YAML file for humans editing it.
--
-- To onboard a new Resibuilt table:
--   1. Add a new entry under tables: below. No model or macro changes needed.
--   2. Commit + merge. The next DAG run picks it up automatically.
--
-- Consumers:
--   * resibuilt_load_tables
--   * resibuilt_quality_checks
--   * resibuilt_resolve_env   (reads source_schema / target_schema / audit_schema)
-- =============================================================================

{% macro resibuilt_table_config() %}

{% set cfg_yaml %}
version: "1.0"
source_schema: RESIBUILT
target_schema: RESIBUILT
audit_schema:  RESIBUILT

tables:

  # ---------------------------------------------------------------------------
  - table_name: PROPERTY
    enabled: true
    business_domain: property_management
    pii_flag: false
    load_strategy: incremental
    watermark_col: UPDATED_DT
    primary_keys: [PROPERTY_ID]
    rcz_cluster_by: MARKET_CODE
    transforms:
      - { source_col: PROP_ID,      target_col: PROPERTY_ID,     cast: "VARCHAR(20)"   }
      - { source_col: MKT_CD,       target_col: MARKET_CODE,     cast: "VARCHAR(10)"   }
      - { source_col: SQFT,         target_col: SQUARE_FOOTAGE,  cast: "INTEGER"       }
      - { source_col: BED_CNT,      target_col: BEDROOM_COUNT,   cast: "INTEGER"       }
      - { source_col: BATH_CNT,     target_col: BATHROOM_COUNT,  cast: "FLOAT"         }
      - { source_col: LIST_PRICE,   target_col: LIST_PRICE_USD,  cast: "NUMBER(12,2)"  }
      - { source_col: PROP_STATUS,  target_col: PROPERTY_STATUS, cast: "VARCHAR(50)"   }
      - { source_col: CREATED_DT,   target_col: CREATED_AT,      cast: "TIMESTAMP_NTZ" }
      - { source_col: UPDATED_DT,   target_col: UPDATED_AT,      cast: "TIMESTAMP_NTZ" }
    quality_checks:
      - { type: not_null, columns: [PROPERTY_ID, MARKET_CODE, PROPERTY_STATUS], severity: error }
      - { type: unique,   columns: [PROPERTY_ID],                                severity: error }
      - { type: range,    column: SQUARE_FOOTAGE, min: 100, max: 20000,          severity: warn  }
      - { type: range,    column: LIST_PRICE_USD, min: 0,   max: 5000000,        severity: warn  }

  # ---------------------------------------------------------------------------
  - table_name: RESIDENT
    enabled: true
    business_domain: resident_management
    pii_flag: true
    load_strategy: full_refresh
    primary_keys: [RESIDENT_ID]
    rcz_cluster_by: LEASE_START_DATE
    transforms:
      - { source_col: RES_ID,         target_col: RESIDENT_ID,      cast: "VARCHAR(20)"  }
      - { source_col: PROP_ID,        target_col: PROPERTY_ID,      cast: "VARCHAR(20)"  }
      - { source_col: FIRST_NM,       target_col: FIRST_NAME,       cast: "VARCHAR(100)", pii_mask: SHA256 }
      - { source_col: LAST_NM,        target_col: LAST_NAME,        cast: "VARCHAR(100)", pii_mask: SHA256 }
      - { source_col: EMAIL_ADDR,     target_col: EMAIL_ADDRESS,    cast: "VARCHAR(255)", pii_mask: SHA256 }
      - { source_col: PHONE_NUM,      target_col: PHONE_NUMBER,     cast: "VARCHAR(20)",  pii_mask: SHA256 }
      - { source_col: LEASE_START_DT, target_col: LEASE_START_DATE, cast: "DATE"         }
      - { source_col: LEASE_END_DT,   target_col: LEASE_END_DATE,   cast: "DATE"         }
      - { source_col: MONTHLY_RENT,   target_col: MONTHLY_RENT_USD, cast: "NUMBER(10,2)" }
    quality_checks:
      - { type: not_null,    columns: [RESIDENT_ID, PROPERTY_ID, LEASE_START_DATE], severity: error }
      - { type: unique,      columns: [RESIDENT_ID],                                 severity: error }
      - { type: referential, column: PROPERTY_ID, ref_table: PROPERTY, ref_column: PROPERTY_ID, severity: warn }
      - { type: range,       column: MONTHLY_RENT_USD, min: 0, max: 50000,           severity: warn }

  # ---------------------------------------------------------------------------
  - table_name: WORK_ORDER
    enabled: true
    business_domain: maintenance
    pii_flag: false
    load_strategy: incremental
    watermark_col: UPDATED_AT
    primary_keys: [WORK_ORDER_ID]
    rcz_cluster_by: CREATED_DATE
    transforms:
      - { source_col: WO_ID,          target_col: WORK_ORDER_ID,      cast: "VARCHAR(30)"   }
      - { source_col: PROP_ID,        target_col: PROPERTY_ID,        cast: "VARCHAR(20)"   }
      - { source_col: WO_TYPE,        target_col: WORK_ORDER_TYPE,    cast: "VARCHAR(50)"   }
      - { source_col: WO_STATUS,      target_col: WORK_ORDER_STATUS,  cast: "VARCHAR(30)"   }
      - { source_col: PRIORITY_CD,    target_col: PRIORITY_CODE,      cast: "VARCHAR(10)"   }
      - { source_col: CREATED_DT,     target_col: CREATED_DATE,       cast: "DATE"          }
      - { source_col: COMPLETED_DT,   target_col: COMPLETED_DATE,     cast: "DATE"          }
      - { source_col: ESTIMATED_COST, target_col: ESTIMATED_COST_USD, cast: "NUMBER(10,2)"  }
      - { source_col: ACTUAL_COST,    target_col: ACTUAL_COST_USD,    cast: "NUMBER(10,2)"  }
      - { source_col: VENDOR_ID,      target_col: VENDOR_ID,          cast: "VARCHAR(20)"   }
      - { source_col: UPDATED_AT,     target_col: LAST_UPDATED_AT,    cast: "TIMESTAMP_NTZ" }
      - { derived: true, target_col: DAYS_TO_COMPLETE,  expression: "DATEDIFF('day', CREATED_DATE, COMPLETED_DATE)" }
      - { derived: true, target_col: COST_VARIANCE_USD, expression: "ACTUAL_COST_USD - ESTIMATED_COST_USD"          }
    quality_checks:
      - { type: not_null,        columns: [WORK_ORDER_ID, PROPERTY_ID, WORK_ORDER_STATUS], severity: error }
      - { type: unique,          columns: [WORK_ORDER_ID],                                  severity: error }
      - { type: accepted_values, column: WORK_ORDER_STATUS, values: [OPEN, IN_PROGRESS, COMPLETED, CANCELLED, ON_HOLD], severity: warn }
      - { type: accepted_values, column: PRIORITY_CODE,     values: [P1, P2, P3, P4], severity: warn }
      - { type: range,           column: ACTUAL_COST_USD,   min: 0, max: 500000,      severity: warn }

  # ---------------------------------------------------------------------------
  - table_name: LEASE
    enabled: true
    business_domain: leasing
    pii_flag: false
    load_strategy: incremental
    watermark_col: MODIFIED_TS
    primary_keys: [LEASE_ID]
    rcz_cluster_by: LEASE_START_DATE
    transforms:
      - { source_col: LEASE_ID,       target_col: LEASE_ID,             cast: "VARCHAR(30)"   }
      - { source_col: PROP_ID,        target_col: PROPERTY_ID,          cast: "VARCHAR(20)"   }
      - { source_col: PRIMARY_RES_ID, target_col: PRIMARY_RESIDENT_ID,  cast: "VARCHAR(20)"   }
      - { source_col: LEASE_TYPE,     target_col: LEASE_TYPE,           cast: "VARCHAR(30)"   }
      - { source_col: LEASE_STAT,     target_col: LEASE_STATUS,         cast: "VARCHAR(20)"   }
      - { source_col: START_DT,       target_col: LEASE_START_DATE,     cast: "DATE"          }
      - { source_col: END_DT,         target_col: LEASE_END_DATE,       cast: "DATE"          }
      - { source_col: MTH_RENT_AMT,   target_col: MONTHLY_RENT_USD,     cast: "NUMBER(10,2)"  }
      - { source_col: SECURITY_DEP,   target_col: SECURITY_DEPOSIT_USD, cast: "NUMBER(10,2)"  }
      - { source_col: MODIFIED_TS,    target_col: MODIFIED_TIMESTAMP,   cast: "TIMESTAMP_NTZ" }
      - { derived: true, target_col: LEASE_TERM_MONTHS, expression: "DATEDIFF('month', LEASE_START_DATE, LEASE_END_DATE)" }
      - { derived: true, target_col: IS_ACTIVE_LEASE,   expression: "CASE WHEN LEASE_STATUS = 'ACTIVE' AND CURRENT_DATE() BETWEEN LEASE_START_DATE AND LEASE_END_DATE THEN TRUE ELSE FALSE END" }
    quality_checks:
      - { type: not_null,        columns: [LEASE_ID, PROPERTY_ID, LEASE_START_DATE, LEASE_END_DATE], severity: error }
      - { type: unique,          columns: [LEASE_ID],                                                 severity: error }
      - { type: accepted_values, column: LEASE_STATUS, values: [ACTIVE, EXPIRED, TERMINATED, PENDING, RENEWED], severity: warn }
      - { type: referential,     column: PROPERTY_ID,         ref_table: PROPERTY, ref_column: PROPERTY_ID, severity: warn }
      - { type: referential,     column: PRIMARY_RESIDENT_ID, ref_table: RESIDENT, ref_column: RESIDENT_ID, severity: warn }
      - { type: range,           column: MONTHLY_RENT_USD,    min: 500, max: 50000,                         severity: warn }

  # ---------------------------------------------------------------------------
  - table_name: INSPECTION
    enabled: true
    business_domain: property_management
    pii_flag: false
    load_strategy: incremental
    watermark_col: INSP_TS
    primary_keys: [INSPECTION_ID]
    rcz_cluster_by: INSPECTION_DATE
    transforms:
      - { source_col: INSP_ID,        target_col: INSPECTION_ID,        cast: "VARCHAR(30)"    }
      - { source_col: PROP_ID,        target_col: PROPERTY_ID,          cast: "VARCHAR(20)"    }
      - { source_col: INSP_TYPE,      target_col: INSPECTION_TYPE,      cast: "VARCHAR(50)"    }
      - { source_col: INSP_DT,        target_col: INSPECTION_DATE,      cast: "DATE"           }
      - { source_col: INSP_TS,        target_col: INSPECTION_TIMESTAMP, cast: "TIMESTAMP_NTZ"  }
      - { source_col: INSPECTOR_ID,   target_col: INSPECTOR_ID,         cast: "VARCHAR(20)"    }
      - { source_col: OVERALL_SCORE,  target_col: OVERALL_SCORE,        cast: "NUMBER(5,2)"    }
      - { source_col: INTERIOR_SCORE, target_col: INTERIOR_SCORE,       cast: "NUMBER(5,2)"    }
      - { source_col: EXTERIOR_SCORE, target_col: EXTERIOR_SCORE,       cast: "NUMBER(5,2)"    }
      - { source_col: PASS_FAIL_IND,  target_col: PASS_FAIL_INDICATOR,  cast: "VARCHAR(4)"     }
      - { source_col: NOTES_TXT,      target_col: INSPECTION_NOTES,     cast: "VARCHAR(4000)"  }
      - { derived: true, target_col: INSPECTION_GRADE, expression: "CASE WHEN OVERALL_SCORE >= 90 THEN 'A' WHEN OVERALL_SCORE >= 80 THEN 'B' WHEN OVERALL_SCORE >= 70 THEN 'C' WHEN OVERALL_SCORE >= 60 THEN 'D' ELSE 'F' END" }
    quality_checks:
      - { type: not_null,        columns: [INSPECTION_ID, PROPERTY_ID, INSPECTION_DATE], severity: error }
      - { type: unique,          columns: [INSPECTION_ID],                                severity: error }
      - { type: accepted_values, column: PASS_FAIL_INDICATOR, values: [PASS, FAIL, "N/A"], severity: warn }
      - { type: range,           column: OVERALL_SCORE,       min: 0, max: 100,            severity: warn }
      - { type: referential,     column: PROPERTY_ID,         ref_table: PROPERTY, ref_column: PROPERTY_ID, severity: warn }
{% endset %}

    {{ return(fromyaml(cfg_yaml)) }}

{% endmacro %}
