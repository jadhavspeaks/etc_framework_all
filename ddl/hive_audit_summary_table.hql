-- DDL for etl_framework_audit.audit_summary table
-- This table stores a high-level summary for each job run.

CREATE EXTERNAL TABLE IF NOT EXISTS etl_framework_audit.audit_summary (
    job_name STRING COMMENT 'Name of the ETL job',
    run_datetime TIMESTAMP COMMENT 'Timestamp when the job instance started',
    run_status STRING COMMENT 'Final status of the job run (e.g., STARTED, SUCCESS, FAILED, SKIPPED_INACTIVE)',
    records_read BIGINT COMMENT 'Optional: Count of records read from source or main input to transformation',
    records_written BIGINT COMMENT 'Optional: Count of records written to the final target',
    run_duration_ms BIGINT COMMENT 'Optional: Total execution time in milliseconds',
    error_message_short STRING COMMENT 'Optional: A short summary or class of error if the job failed',
    audit_event_datetime TIMESTAMP COMMENT 'Timestamp of this specific audit event (e.g., when the status was logged)'
)
PARTITIONED BY (
    run_date STRING COMMENT 'Date of the job run (YYYY-MM-DD), derived from run_datetime',
    job_name_part STRING COMMENT 'Job name, for partitioning. Can be redundant if job_name column is already used effectively in queries, but common for HDFS layout.'
)
STORED AS PARQUET
LOCATION '/etl_framework/audit_logs/audit_summary'; -- Example HDFS location, user should configure appropriately

-- Note: User needs to create the database `etl_framework_audit` separately.
-- e.g.: CREATE DATABASE IF NOT EXISTS etl_framework_audit;
-- For managed tables, remove EXTERNAL and LOCATION clauses and let Hive manage the data path.
-- Consider table properties like TBLPROPERTIES ('orc.compress'='ZLIB') if using ORC or other specific settings.
