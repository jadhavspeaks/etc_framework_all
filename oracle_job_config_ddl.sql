CREATE TABLE JOB_CONFIG (
    -- Core Job Identification
    job_name VARCHAR2(255) NOT NULL PRIMARY KEY, -- Changed: job_name is now PK, job_id removed
    job_description VARCHAR2(1000),
    is_active CHAR(1) DEFAULT 'Y' NOT NULL CHECK (is_active IN ('Y', 'N')), -- Flag to enable/disable the job

    -- Source Configuration
    source_type VARCHAR2(50) NOT NULL, -- e.g., 'FILE', 'KAFKA', 'JDBC', 'HIVE_TABLE'
    source_connection_details VARCHAR2(4000), -- JDBC URL, Kafka brokers, file URI base path, Hive db.table
    source_format VARCHAR2(50), -- e.g., 'CSV', 'PARQUET', 'JSON', 'AVRO', 'TEXT' (for Kafka string messages)
    source_format_options VARCHAR2(1000), -- JSON string for options: e.g., {'delimiter': ',', 'header': 'true'} for CSV; Kafka topic name
    source_schema CLOB, -- Can store JSON schema, Avro schema, or DDL for a temporary view

    -- Target Configuration
    target_type VARCHAR2(50) NOT NULL, -- e.g., 'HDFS', 'ORACLE_TABLE', 'KAFKA_TOPIC', 'HIVE_TABLE'
    target_connection_details VARCHAR2(4000), -- JDBC URL, HDFS path, Kafka brokers, Hive db.table
    target_format VARCHAR2(50), -- e.g., 'PARQUET', 'ORC', 'CSV', 'AVRO'
    target_format_options VARCHAR2(1000), -- JSON string for options: e.g., {'compression': 'snappy'}
    target_table_or_path VARCHAR2(1000) NOT NULL, -- Table name, HDFS directory, Kafka topic
    load_type VARCHAR2(50) DEFAULT 'INCREMENTAL' NOT NULL, -- 'INCREMENTAL', 'FULL_RELOAD'
    target_write_mode VARCHAR2(50) DEFAULT 'overwrite' NOT NULL, -- Spark save mode: 'overwrite', 'append', 'ignore', 'errorifexists'

    -- Transformation Logic
    transformation_mode VARCHAR2(50) DEFAULT 'AS_IS' NOT NULL, -- 'AS_IS', 'WITH_LOGIC', 'SCD2'
    sql_logic CLOB, -- SQL query for 'WITH_LOGIC' and 'SCD2' modes
    schema_mapping_logic CLOB, -- JSON defining column mappings, renames, type casts e.g. {'colA': 'target_col_A', 'colB': {'target_name': 'target_col_B', 'type': 'integer', 'default': 0}}

    -- SCD Type 2 Specifics (nullable as they only apply to SCD2 mode)
    scd2_natural_keys VARCHAR2(1000), -- Comma-separated list of columns forming the natural key
    scd2_change_tracking_column VARCHAR2(100), -- Source column indicating last update time or version
    scd2_surrogate_key_column VARCHAR2(100) DEFAULT 'sk_id',
    scd2_valid_from_column VARCHAR2(100) DEFAULT 'valid_from_ts',
    scd2_valid_to_column VARCHAR2(100) DEFAULT 'valid_to_ts',
    scd2_version_column VARCHAR2(100) DEFAULT 'version',
    scd2_current_flag_column VARCHAR2(100) DEFAULT 'is_current_flag', -- Y/N char or boolean true/false

    -- Execution Control & Dependencies
    partitioning_columns VARCHAR2(1000), -- Comma-separated list for target partitioning
    job_priority NUMBER DEFAULT 0, -- Lower number means higher priority
    max_retries NUMBER DEFAULT 3,
    retry_backoff_ms NUMBER DEFAULT 60000, -- e.g., 1 minute
    sla_threshold_minutes NUMBER, -- SLA for job completion in minutes
    dependency_job_ids VARCHAR2(4000), -- Comma-separated list of job_names this job depends on (was job_ids)

    -- Auditing & Reconciliation
    audit_level VARCHAR2(50) DEFAULT 'JOB' NOT NULL, -- 'NONE', 'JOB' (summary), 'RECORD' (detailed, if supported)
    reconciliation_enabled CHAR(1) DEFAULT 'N' NOT NULL CHECK (reconciliation_enabled IN ('Y', 'N')),
    reconciliation_config CLOB, -- JSON with reconciliation details

    -- Data Quality
    dq_checks_enabled CHAR(1) DEFAULT 'N' NOT NULL CHECK (dq_checks_enabled IN ('Y', 'N')),
    dq_rules_config CLOB, -- JSON array of DQ rules

    -- Notifications
    notification_emails_success VARCHAR2(4000),
    notification_emails_failure VARCHAR2(4000),
    notification_verbosity VARCHAR2(50) DEFAULT 'BRIEF' CHECK (notification_verbosity IN ('BRIEF', 'DETAILED')),

    -- Job Properties (Generic JSON for additional settings)
    job_properties CLOB,

    -- Security & Framework Metadata
    log_masking_columns VARCHAR2(1000),
    execution_authorization_flag CHAR(1) DEFAULT 'Y' NOT NULL CHECK (execution_authorization_flag IN ('Y', 'N')),
    manual_trigger_only CHAR(1) DEFAULT 'N' NOT NULL CHECK (manual_trigger_only IN ('Y', 'N')),
    created_by VARCHAR2(100) DEFAULT USER NOT NULL,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR2(100) DEFAULT USER NOT NULL,
    updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    config_version NUMBER DEFAULT 1 NOT NULL
);

COMMENT ON TABLE JOB_CONFIG IS 'Central configuration table for all ETL jobs.';
COMMENT ON COLUMN JOB_CONFIG.job_name IS 'Unique human-readable name for the ETL job. Serves as Primary Key.';
COMMENT ON COLUMN JOB_CONFIG.is_active IS 'Flag to enable (Y) or disable (N) the job. Default is Y.';
COMMENT ON COLUMN JOB_CONFIG.source_type IS 'Type of data source (e.g., FILE, KAFKA, JDBC, HIVE_TABLE).';
COMMENT ON COLUMN JOB_CONFIG.target_type IS 'Type of data target (e.g., HDFS, ORACLE_TABLE, KAFKA_TOPIC, HIVE_TABLE).';
COMMENT ON COLUMN JOB_CONFIG.dependency_job_ids IS 'Comma-separated list of job_names this job depends on.';
COMMENT ON COLUMN JOB_CONFIG.job_properties IS 'JSON CLOB for storing additional, non-standard job properties or flags (e.g., schema_drift_new_source_columns_behavior, scd2_handle_deletes_by_absence).';
