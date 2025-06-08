-- DDL for etl_framework_audit.rejection_events table
-- This table stores records of data rejections that occurred during job execution (e.g., from DQ checks).

CREATE EXTERNAL TABLE IF NOT EXISTS etl_framework_audit.rejection_events (
    job_name STRING COMMENT 'Name of the ETL job that produced this rejection',
    run_datetime TIMESTAMP COMMENT 'Timestamp of the job run start that produced this rejection',
    rejection_timestamp TIMESTAMP COMMENT 'Timestamp when the rejection event actually occurred',
    stage STRING COMMENT 'ETL stage where the rejection occurred (e.g., DQValidation, SchemaMapping)',
    record_identifier STRING COMMENT 'Optional: Natural keys or other unique identifier of the rejected record',
    reason STRING COMMENT 'Reason why the record was rejected',
    rejected_data STRING COMMENT 'Optional: JSON or delimited string representation of the rejected row (potentially truncated/masked)'
)
PARTITIONED BY (
    rejection_date STRING COMMENT 'Date of the rejection event (YYYY-MM-DD), derived from rejection_timestamp',
    job_name_part STRING COMMENT 'Job name, for partitioning.'
)
STORED AS PARQUET
LOCATION '/etl_framework/audit_logs/rejection_events'; -- Example HDFS location

-- Note: Assumes database `etl_framework_audit` exists.
