-- DDL for etl_framework_audit.job_run_details table
-- This table stores granular details for each job run, especially for debugging and deeper analysis.

CREATE EXTERNAL TABLE IF NOT EXISTS etl_framework_audit.job_run_details (
    job_name STRING COMMENT 'Name of the ETL job',
    run_datetime TIMESTAMP COMMENT 'Timestamp when the job instance started (links to audit_summary)',
    spark_app_id STRING COMMENT 'Spark Application ID for this job run',
    spark_tracking_url STRING COMMENT 'Placeholder for Spark UI tracking URL; may contain YARN app ID or be environment specific',
    job_config_json STRING COMMENT 'JSON representation of the JobConfig used for this run',
    full_stack_trace STRING COMMENT 'Optional: Complete stack trace if the job failed',
    reconciliation_report_json STRING COMMENT 'Optional: JSON representation of the ReconciliationReport',
    dq_report_json STRING COMMENT 'Optional: JSON representation of Data Quality results', -- Added column
    custom_job_properties_json STRING COMMENT 'Optional: JSON of job_properties from JobConfig'
)
PARTITIONED BY (
    run_date STRING COMMENT 'Date of the job run (YYYY-MM-DD), derived from run_datetime',
    job_name_part STRING COMMENT 'Job name, for partitioning.'
)
STORED AS PARQUET
LOCATION '/etl_framework/audit_logs/job_run_details'; -- Example HDFS location

-- Note: Assumes database `etl_framework_audit` exists.
