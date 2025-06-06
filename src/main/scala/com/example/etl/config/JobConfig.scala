package com.example.etl.config

import java.sql.Timestamp

// Using Option for fields that can be null in the database or are specific to certain modes
case class JobConfig(
    job_id: String,
    job_name: String,
    job_description: Option[String],
    is_active: String, // 'Y' or 'N'

    source_type: String,
    source_connection_details: Option[String],
    source_format: Option[String],
    source_format_options: Option[String],
    source_schema: Option[String],

    target_type: String,
    target_connection_details: Option[String],
    target_format: Option[String],
    target_format_options: Option[String],
    target_table_or_path: String,
    load_type: String,
    target_write_mode: String,

    transformation_mode: String,
    sql_logic: Option[String],
    schema_mapping_logic: Option[String],

    scd2_natural_keys: Option[String],
    scd2_change_tracking_column: Option[String],
    scd2_surrogate_key_column: Option[String],
    scd2_valid_from_column: Option[String],
    scd2_valid_to_column: Option[String],
    scd2_version_column: Option[String],
    scd2_current_flag_column: Option[String],

    partitioning_columns: Option[String],
    job_priority: Option[Int],
    max_retries: Option[Int],
    retry_backoff_ms: Option[Long],
    sla_threshold_minutes: Option[Int],
    dependency_job_ids: Option[String],

    audit_level: String,
    reconciliation_enabled: String, // 'Y' or 'N'
    reconciliation_config: Option[String],

    dq_checks_enabled: String, // 'Y' or 'N'
    dq_rules_config: Option[String],

    notification_emails_success: Option[String],
    notification_emails_failure: Option[String],
    notification_verbosity: Option[String],

    log_masking_columns: Option[String],
    execution_authorization_flag: String, // 'Y' or 'N'
    manual_trigger_only: String, // 'Y' or 'N'

    created_by: String,
    created_ts: Timestamp,
    updated_by: String,
    updated_ts: Timestamp,
    config_version: Int
)
