CREATE TABLE JOB_CONFIG (
    -- Core Job Identification
    job_id VARCHAR2(100) NOT NULL PRIMARY KEY,
    job_name VARCHAR2(255) NOT NULL,
    job_description VARCHAR2(1000),
    is_active CHAR(1) DEFAULT 'Y' NOT NULL CHECK (is_active IN ('Y', 'N')), -- Flag to enable/disable the job

    -- Source Configuration
    source_type VARCHAR2(50) NOT NULL, -- e.g., 'FILE', 'KAFKA', 'JDBC'
    source_connection_details VARCHAR2(4000), -- JDBC URL, Kafka brokers, file URI base path
    source_format VARCHAR2(50), -- e.g., 'CSV', 'PARQUET', 'JSON', 'AVRO', 'TEXT' (for Kafka string messages)
    source_format_options VARCHAR2(1000), -- JSON string for options: e.g., {'delimiter': ',', 'header': 'true'} for CSV; Kafka topic name
    source_schema CLOB, -- Can store JSON schema, Avro schema, or DDL for a temporary view

    -- Target Configuration
    target_type VARCHAR2(50) NOT NULL, -- e.g., 'HDFS', 'ORACLE_TABLE', 'KAFKA_TOPIC'
    target_connection_details VARCHAR2(4000), -- JDBC URL, HDFS path, Kafka brokers
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
    scd2_current_flag_column VARCHAR2(100) DEFAULT 'is_current_flag', -- Y/N char

    -- Execution Control & Dependencies
    partitioning_columns VARCHAR2(1000), -- Comma-separated list for target partitioning
    job_priority NUMBER DEFAULT 0, -- Lower number means higher priority
    max_retries NUMBER DEFAULT 3,
    retry_backoff_ms NUMBER DEFAULT 60000, -- e.g., 1 minute
    sla_threshold_minutes NUMBER, -- SLA for job completion in minutes
    dependency_job_ids VARCHAR2(4000), -- Comma-separated list of job_ids this job depends on

    -- Auditing & Reconciliation
    audit_level VARCHAR2(50) DEFAULT 'JOB' NOT NULL, -- 'NONE', 'JOB' (summary), 'RECORD' (detailed, if supported)
    reconciliation_enabled CHAR(1) DEFAULT 'N' NOT NULL CHECK (reconciliation_enabled IN ('Y', 'N')),
    reconciliation_config CLOB, -- JSON with reconciliation details: e.g. {'type': 'count_sum', 'columns_to_sum': ['amount'], 'tolerance_percent': 0.01}

    -- Data Quality
    dq_checks_enabled CHAR(1) DEFAULT 'N' NOT NULL CHECK (dq_checks_enabled IN ('Y', 'N')),
    dq_rules_config CLOB, -- JSON array of DQ rules: e.g., [{'rule_name': 'col_A_not_null', 'type': 'NOT_NULL', 'column': 'col_A'}, {'type': 'REGEX', 'column': 'email', 'pattern': '...'}]

    -- Notifications
    notification_emails_success VARCHAR2(4000), -- Comma-separated email list
    notification_emails_failure VARCHAR2(4000), -- Comma-separated email list
    notification_verbosity VARCHAR2(50) DEFAULT 'BRIEF' CHECK (notification_verbosity IN ('BRIEF', 'DETAILED')),

    -- Security & Framework Metadata
    log_masking_columns VARCHAR2(1000), -- Comma-separated list of columns to mask in logs
    execution_authorization_flag CHAR(1) DEFAULT 'Y' NOT NULL CHECK (execution_authorization_flag IN ('Y', 'N')), -- Requires explicit auth to run
    manual_trigger_only CHAR(1) DEFAULT 'N' NOT NULL CHECK (manual_trigger_only IN ('Y', 'N')), -- If 'Y', cannot be run by automated scheduler based on dependencies
    created_by VARCHAR2(100) DEFAULT USER NOT NULL,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR2(100) DEFAULT USER NOT NULL,
    updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    config_version NUMBER DEFAULT 1 NOT NULL -- For optimistic locking or tracking changes to this job's configuration
);

COMMENT ON TABLE JOB_CONFIG IS 'Central configuration table for all ETL jobs.';
COMMENT ON COLUMN JOB_CONFIG.job_id IS 'Unique identifier for the ETL job.';
COMMENT ON COLUMN JOB_CONFIG.job_name IS 'Human-readable name for the job.';
COMMENT ON COLUMN JOB_CONFIG.job_description IS 'Detailed description of the job''s purpose.';
COMMENT ON COLUMN JOB_CONFIG.is_active IS 'Flag to enable (Y) or disable (N) the job. Default is Y.';

COMMENT ON COLUMN JOB_CONFIG.source_type IS 'Type of data source (e.g., ''FILE'', ''KAFKA'', ''JDBC'').';
COMMENT ON COLUMN JOB_CONFIG.source_connection_details IS 'Connection string or details for the source (e.g., JDBC URL, Kafka brokers, base file path).';
COMMENT ON COLUMN JOB_CONFIG.source_format IS 'Data format of the source (e.g., ''CSV'', ''PARQUET'', ''JSON'', ''AVRO''). For Kafka, can be topic name or message format like ''TEXT''.';
COMMENT ON COLUMN JOB_CONFIG.source_format_options IS 'JSON string for source-specific options (e.g., CSV delimiter, header flag, Kafka topic).';
COMMENT ON COLUMN JOB_CONFIG.source_schema IS 'Schema definition for the source data (e.g., JSON schema, Avro schema string, or DDL for creating a temporary view).';

COMMENT ON COLUMN JOB_CONFIG.target_type IS 'Type of data target (e.g., ''HDFS'', ''ORACLE_TABLE'', ''KAFKA_TOPIC'').';
COMMENT ON COLUMN JOB_CONFIG.target_connection_details IS 'Connection string or details for the target (e.g., JDBC URL, HDFS base path).';
COMMENT ON COLUMN JOB_CONFIG.target_format IS 'Data format for the target (e.g., ''PARQUET'', ''ORC'', ''CSV'').';
COMMENT ON COLUMN JOB_CONFIG.target_format_options IS 'JSON string for target-specific options (e.g., compression codec).';
COMMENT ON COLUMN JOB_CONFIG.target_table_or_path IS 'Name of the target table, HDFS directory, or Kafka topic.';
COMMENT ON COLUMN JOB_CONFIG.load_type IS 'Type of data load (''INCREMENTAL'', ''FULL_RELOAD''). Default is INCREMENTAL.';
COMMENT ON COLUMN JOB_CONFIG.target_write_mode IS 'Spark DataFrame write mode (e.g., ''overwrite'', ''append''). Default is overwrite.';

COMMENT ON COLUMN JOB_CONFIG.transformation_mode IS 'Transformation mode for the job (''AS_IS'', ''WITH_LOGIC'', ''SCD2''). Default is AS_IS.';
COMMENT ON COLUMN JOB_CONFIG.sql_logic IS 'SQL query used for transformations in ''WITH_LOGIC'' and ''SCD2'' modes.';
COMMENT ON COLUMN JOB_CONFIG.schema_mapping_logic IS 'JSON defining explicit column mappings, renames, data type casts, and default values.';

COMMENT ON COLUMN JOB_CONFIG.scd2_natural_keys IS 'Comma-separated list of columns that form the natural key for SCD Type 2 records.';
COMMENT ON COLUMN JOB_CONFIG.scd2_change_tracking_column IS 'Source column used to detect changes for SCD Type 2 (e.g., last_modified_date).';
COMMENT ON COLUMN JOB_CONFIG.scd2_surrogate_key_column IS 'Name of the surrogate key column to be generated for SCD Type 2. Default is sk_id.';
COMMENT ON COLUMN JOB_CONFIG.scd2_valid_from_column IS 'Name of the column storing the start date/timestamp of record validity for SCD Type 2. Default is valid_from_ts.';
COMMENT ON COLUMN JOB_CONFIG.scd2_valid_to_column IS 'Name of the column storing the end date/timestamp of record validity for SCD Type 2. Default is valid_to_ts.';
COMMENT ON COLUMN JOB_CONFIG.scd2_version_column IS 'Name of the column storing the version number of the record for SCD Type 2. Default is version.';
COMMENT ON COLUMN JOB_CONFIG.scd2_current_flag_column IS 'Name of the column indicating if the SCD Type 2 record is the current version (Y/N). Default is is_current_flag.';

COMMENT ON COLUMN JOB_CONFIG.partitioning_columns IS 'Comma-separated list of columns to be used for partitioning the target data.';
COMMENT ON COLUMN JOB_CONFIG.job_priority IS 'Priority of the job for execution (lower number means higher priority). Default is 0.';
COMMENT ON COLUMN JOB_CONFIG.max_retries IS 'Maximum number of times the job should be retried upon failure. Default is 3.';
COMMENT ON COLUMN JOB_CONFIG.retry_backoff_ms IS 'Time in milliseconds to wait before retrying a failed job (can be used for exponential backoff logic). Default is 60000 (1 minute).';
COMMENT ON COLUMN JOB_CONFIG.sla_threshold_minutes IS 'Service Level Agreement: maximum allowed time in minutes for job completion.';
COMMENT ON COLUMN JOB_CONFIG.dependency_job_ids IS 'Comma-separated list of job_ids that must complete successfully before this job can run.';

COMMENT ON COLUMN JOB_CONFIG.audit_level IS 'Level of auditing for the job (''NONE'', ''JOB'' for summary, ''RECORD'' for detailed). Default is JOB.';
COMMENT ON COLUMN JOB_CONFIG.reconciliation_enabled IS 'Flag to enable (Y) or disable (N) data reconciliation for this job. Default is N.';
COMMENT ON COLUMN JOB_CONFIG.reconciliation_config IS 'JSON defining reconciliation parameters (e.g., type, columns for checksum, tolerance).';

COMMENT ON COLUMN JOB_CONFIG.dq_checks_enabled IS 'Flag to enable (Y) or disable (N) Data Quality checks. Default is N.';
COMMENT ON COLUMN JOB_CONFIG.dq_rules_config IS 'JSON array defining Data Quality rules to be applied (e.g., NOT_NULL, REGEX, MIN_MAX).';

COMMENT ON COLUMN JOB_CONFIG.notification_emails_success IS 'Comma-separated list of email addresses for success notifications.';
COMMENT ON COLUMN JOB_CONFIG.notification_emails_failure IS 'Comma-separated list of email addresses for failure notifications.';
COMMENT ON COLUMN JOB_CONFIG.notification_verbosity IS 'Verbosity of notifications (''BRIEF'', ''DETAILED''). Default is BRIEF.';

COMMENT ON COLUMN JOB_CONFIG.log_masking_columns IS 'Comma-separated list of sensitive column names whose values should be masked in logs.';
COMMENT ON COLUMN JOB_CONFIG.execution_authorization_flag IS 'If ''Y'', the job requires explicit authorization to run, preventing accidental execution. Default is Y.';
COMMENT ON COLUMN JOB_CONFIG.manual_trigger_only IS 'If ''Y'', this job can only be triggered manually and not by automated schedulers or dependency chains. Default is N.';
COMMENT ON COLUMN JOB_CONFIG.created_by IS 'User who created the job configuration row. Default is the Oracle USER.';
COMMENT ON COLUMN JOB_CONFIG.created_ts IS 'Timestamp when the job configuration row was created. Default is CURRENT_TIMESTAMP.';
COMMENT ON COLUMN JOB_CONFIG.updated_by IS 'User who last updated the job configuration row. Default is the Oracle USER.';
COMMENT ON COLUMN JOB_CONFIG.updated_ts IS 'Timestamp when the job configuration row was last updated. Default is CURRENT_TIMESTAMP.';
COMMENT ON COLUMN JOB_CONFIG.config_version IS 'Version number for this job configuration, can be used for optimistic locking or auditing changes. Default is 1.';
