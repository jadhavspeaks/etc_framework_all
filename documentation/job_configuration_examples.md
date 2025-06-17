# ETL Job Configuration Examples

This document provides example configurations for various types of ETL jobs defined in the `JOB_CONFIG` Oracle table. These examples illustrate how to use different features of the framework.

**Note**: For clarity, only relevant fields for each example are shown. Mandatory fields like `job_name`, `is_active`, `source_type`, `target_type`, etc., are always required. CLOB fields (`sql_logic`, `schema_mapping_logic`, etc.) would store the JSON/SQL as a string.

## 1. AS_IS Mode: File (CSV) to HDFS (Parquet) with Schema Mapping

This example shows an AS_IS job that reads a CSV file, applies schema transformations (renaming, type casting, default values), and writes the output as Parquet to HDFS.

**JOB_CONFIG Row (Conceptual - relevant fields):**

| Field Name                          | Example Value                                                                     |
|-------------------------------------|-----------------------------------------------------------------------------------|
| `job_name`                          | `ingest_customer_csv_to_hdfs_parquet`                                               |
| `job_description`                   | `Loads customer data from CSV, applies schema, writes to Parquet in HDFS.`          |
| `is_active`                         | `Y`                                                                               |
| `source_type`                       | `FILE`                                                                            |
| `source_connection_details`         | `/mnt/landing/customers/customer_data_YYYYMMDD.csv` (path might include date patterns) |
| `source_format`                     | `CSV`                                                                             |
| `source_format_options`             | `{"header": "true", "delimiter": ",", "inferSchema": "false"}`                      |
| `target_type`                       | `HDFS`                                                                            |
| `target_table_or_path`              | `/data/processed/customers_parquet`                                                 |
| `target_format`                     | `PARQUET`                                                                         |
| `target_write_mode`                 | `overwrite`                                                                       |
| `transformation_mode`               | `AS_IS`                                                                           |
| `schema_mapping_logic` (CLOB)       | See JSON below                                                                    |
| `job_properties` (CLOB)             | `{"schema_drift_new_source_columns_behavior": "ignore"}`                         |
| `log_masking_columns`               | `email_address,phone_number`                                                      |
| `audit_level`                       | `JOB`                                                                             |
| `reconciliation_enabled`           | `Y`                                                                               |
| `reconciliation_config` (CLOB)     | `{"checks":[{"type":"record_count", "tolerance_percent":0}]}`                  |
| `dq_checks_enabled`                 | `Y`                                                                               |
| `dq_rules_config` (CLOB)            | See DQ JSON Example below                                                         |

**`schema_mapping_logic` JSON Example:**
```json
{
  "CUST_ID": { "target_name": "customer_id", "target_type": "long" },
  "FNAME": { "target_name": "first_name", "target_type": "string" },
  "LNAME": { "target_name": "last_name", "target_type": "string" },
  "EMAIL": { "target_name": "email_address", "target_type": "string" },
  "REG_DATE": { "target_name": "registration_date", "target_type": "date", "date_format": "yyyy-MM-dd" },
  "COUNTRY_CD": { "target_name": "country_code", "target_type": "string", "default_if_null": "US" },
  "__NEW_COLUMN__STATUS": { "target_name": "status_flag", "target_type": "string", "default_value": "ACTIVE" }
}
```

**`dq_rules_config` JSON Example (for the target Parquet data):**
```json
{
  "rules": [
    { "rule_name": "customer_id_not_null", "rule_type": "NOT_NULL", "columns": ["customer_id"], "error_level": "FAIL_JOB" },
    { "rule_name": "target_row_count", "rule_type": "ROW_COUNT", "expression": "{\"min\": 1}", "error_level": "WARN" }
  ]
}
```

## 2. WITH_LOGIC Mode: Hive Table to Hive Table with SQL Transformation & Parameters
This job reads from a Hive table (presumably loaded by an AS_IS job), applies a SQL transformation that uses runtime parameters, and writes to another Hive table.

**JOB_CONFIG Row (Conceptual - relevant fields):**

| Field Name                          | Example Value                                                                  |
|-------------------------------------|--------------------------------------------------------------------------------|
| `job_name`                          | `transform_daily_sales_summary`                                                |
| `is_active`                         | `Y`                                                                            |
| `source_type`                       | `HIVE_TABLE`                                                                   |
| `source_connection_details`         | `staging_db.raw_sales_events`                                                  |
| `target_type`                       | `HIVE_TABLE`                                                                   |
| `target_table_or_path`              | `analytics_db.daily_product_summary`                                           |
| `target_write_mode`                 | `overwrite`                                                                    |
| `transformation_mode`               | `WITH_LOGIC`                                                                   |
| `sql_logic` (CLOB)                  | See SQL below                                                                  |
| `dependency_job_ids`                | `ingest_raw_sales_events_as_is`                                                |

**`sql_logic` Example (using `spark.etl.param.processing_date` passed via `spark-submit --conf`):**

(Assumes the HIVE_TABLE `staging_db.raw_sales_events` is registered as view `raw_sales_events_source_input_for_logic` by the handler)

```sql
SELECT
    product_id,
    SUM(sale_amount) as total_sales_amount,
    COUNT(DISTINCT transaction_id) as distinct_transactions,
    '${processing_date}' as processing_date -- Parameter substitution
FROM
    ${job_name}_source_input_for_logic -- View name for source_connection_details table
WHERE
    event_date = '${processing_date}' -- Parameter substitution
GROUP BY
    product_id
```
*Note: The view name `${job_name}_source_input_for_logic` is a convention used by `WithLogicIngestionHandler`. If your SQL references other tables, they must already exist in Hive and be accessible by Spark.*

## 3. SCD Type 2 Mode: File (CSV) to Hive Dimension Table
This job loads source data, transforms it using `sql_logic` (optional, could be AS_IS source prep), and then applies SCD Type 2 logic to a Hive dimension table.

**JOB_CONFIG Row (Conceptual - relevant fields):**

| Field Name                          | Example Value                                                                    |
|-------------------------------------|----------------------------------------------------------------------------------|
| `job_name`                          | `load_dim_employee_scd2`                                                           |
| `source_type`                       | `FILE`                                                                           |
| `source_connection_details`         | `/mnt/landing/hr/employee_updates.csv`                                           |
| `source_format`                     | `CSV`                                                                            |
| `source_format_options`             | `{"header": "true"}`                                                               |
| `target_type`                       | `HIVE_TABLE`                                                                     |
| `target_table_or_path`              | `dw_dims.dim_employee`                                                             |
| `transformation_mode`               | `SCD2`                                                                           |
| `sql_logic` (CLOB)                  | `SELECT emp_id, name, department, role, location, salary, last_change_ts FROM ${job_name}_raw_source_for_scd_tfm` (Example: preparing columns) |
| `scd2_natural_keys`                 | `emp_id`                                                                         |
| `scd2_change_tracking_column`       | `last_change_ts`                                                                 |
| `scd2_surrogate_key_column`         | `employee_sk`                                                                    |
| `scd2_valid_from_column`            | `valid_from`                                                                     |
| `scd2_valid_to_column`              | `valid_to`                                                                       |
| `scd2_version_column`               | `version_no`                                                                     |
| `scd2_current_flag_column`          | `is_current_rec`                                                                 |
| `job_properties` (CLOB)             | `{"scd2_handle_deletes_by_absence": "true"}`                                     |

*Note: The target Hive table `dw_dims.dim_employee` must pre-exist with all business columns AND the SCD metadata columns (e.g., `employee_sk`, `valid_from`, `valid_to`, `version_no`, `is_current_rec`, `change_type`).*


## More Examples to be Added
- Kafka source
- JDBC source/target
- Different reconciliation and DQ rule configurations
