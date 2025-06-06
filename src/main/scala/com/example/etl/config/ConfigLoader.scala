package com.example.etl.config

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger
import java.util.Properties

object ConfigLoader {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  // Define constants for Spark configuration keys for Oracle connection
  val OracleUrlKey = "spark.etl.oracle.url"
  val OracleUserKey = "spark.etl.oracle.user"
  val OraclePasswordKey = "spark.etl.oracle.password"
  val OracleDriverKey = "spark.etl.oracle.driver" // e.g., "oracle.jdbc.driver.OracleDriver"

  /**
   * Loads job configuration for a given job_id from the JOB_CONFIG Oracle table.
   *
   * Oracle connection parameters (URL, user, password, driver) are expected
   * to be passed via Spark configuration (e.g., spark-submit --conf key=value).
   *
   * @param jobId The ID of the job to load configuration for.
   * @param spark The active SparkSession.
   * @return A JobConfig object.
   * @throws RuntimeException if configuration is not found or if there's a DB issue.
   */
  def loadConfig(jobId: String, spark: SparkSession): JobConfig = {
    logger.info(s"Loading configuration for job_id: $jobId")

    val oracleUrl = spark.conf.get(OracleUrlKey)
    val oracleUser = spark.conf.get(OracleUserKey)
    val oraclePassword = spark.conf.get(OraclePasswordKey)
    val oracleDriver = spark.conf.get(OracleDriverKey, "oracle.jdbc.driver.OracleDriver") // Default driver

    if (oracleUrl == null || oracleUser == null || oraclePassword == null) {
      val errorMsg = s"Oracle connection properties not fully provided. Set $OracleUrlKey, $OracleUserKey, $OraclePasswordKey in Spark conf."
      logger.error(errorMsg)
      throw new IllegalArgumentException(errorMsg)
    }

    val connectionProperties = new Properties()
    connectionProperties.put("user", oracleUser)
    connectionProperties.put("password", oraclePassword)
    connectionProperties.put("driver", oracleDriver)

    val query = s"(SELECT * FROM JOB_CONFIG WHERE job_id = '$jobId') job_config_query"
    logger.info(s"Executing query: $query against $oracleUrl (user: $oracleUser)")

    try {
      val jobConfigDf: DataFrame = spark.read
        .jdbc(oracleUrl, query, connectionProperties)

      if (jobConfigDf.isEmpty) {
        val errorMsg = s"No configuration found for job_id: $jobId in JOB_CONFIG table."
        logger.error(errorMsg)
        throw new RuntimeException(errorMsg)
      }

      if (jobConfigDf.count() > 1) {
        val warnMsg = s"Multiple configurations found for job_id: $jobId. Using the first one."
        logger.warn(warnMsg)
        // Potentially add more robust handling here, e.g., based on version or fail
      }

      // Convert DataFrame row to JobConfig case class
      // This requires careful mapping of DataFrame columns to case class fields, especially with Option types
      // For simplicity, this example assumes column names match and handles Option types correctly.
      // A more robust solution might involve a library or more explicit mapping.
      import spark.implicits._ // Required for .as[JobConfig]

      // Explicitly select columns to match the case class order and handle potential nulls for Option types
      // This is a simplified mapping. A real implementation would need to be more robust,
      // perhaps iterating through fields or using a helper function.
      // val config = jobConfigDf.as[JobConfig].first() // This direct conversion is often problematic with JDBC DataFrames and complex schemas.
                                                  // A more robust approach is shown below.

      logger.info(s"Successfully loaded configuration for job_id: $jobId")

      // More robust mapping:
      val row = jobConfigDf.first() // Get the first Row

      JobConfig(
        job_id = row.getAs[String]("job_id"),
        job_name = row.getAs[String]("job_name"),
        job_description = Option(row.getAs[String]("job_description")),
        is_active = row.getAs[String]("is_active"),
        source_type = row.getAs[String]("source_type"),
        source_connection_details = Option(row.getAs[String]("source_connection_details")),
        source_format = Option(row.getAs[String]("source_format")),
        source_format_options = Option(row.getAs[String]("source_format_options")),
        source_schema = Option(row.getAs[String]("source_schema")),
        target_type = row.getAs[String]("target_type"),
        target_connection_details = Option(row.getAs[String]("target_connection_details")),
        target_format = Option(row.getAs[String]("target_format")),
        target_format_options = Option(row.getAs[String]("target_format_options")),
        target_table_or_path = row.getAs[String]("target_table_or_path"),
        load_type = row.getAs[String]("load_type"),
        target_write_mode = row.getAs[String]("target_write_mode"),
        transformation_mode = row.getAs[String]("transformation_mode"),
        sql_logic = Option(row.getAs[String]("sql_logic")),
        schema_mapping_logic = Option(row.getAs[String]("schema_mapping_logic")),
        scd2_natural_keys = Option(row.getAs[String]("scd2_natural_keys")),
        scd2_change_tracking_column = Option(row.getAs[String]("scd2_change_tracking_column")),
        scd2_surrogate_key_column = Option(row.getAs[String]("scd2_surrogate_key_column")),
        scd2_valid_from_column = Option(row.getAs[String]("scd2_valid_from_column")),
        scd2_valid_to_column = Option(row.getAs[String]("scd2_valid_to_column")),
        scd2_version_column = Option(row.getAs[String]("scd2_version_column")),
        scd2_current_flag_column = Option(row.getAs[String]("scd2_current_flag_column")),
        partitioning_columns = Option(row.getAs[String]("partitioning_columns")),
        job_priority = if (row.isNullAt(row.fieldIndex("job_priority"))) None else Some(row.getAs[java.math.BigDecimal]("job_priority").intValue()), // Oracle NUMBER maps to BigDecimal
        max_retries = if (row.isNullAt(row.fieldIndex("max_retries"))) None else Some(row.getAs[java.math.BigDecimal]("max_retries").intValue()),
        retry_backoff_ms = if (row.isNullAt(row.fieldIndex("retry_backoff_ms"))) None else Some(row.getAs[java.math.BigDecimal]("retry_backoff_ms").longValue()),
        sla_threshold_minutes = if (row.isNullAt(row.fieldIndex("sla_threshold_minutes"))) None else Some(row.getAs[java.math.BigDecimal]("sla_threshold_minutes").intValue()),
        dependency_job_ids = Option(row.getAs[String]("dependency_job_ids")),
        audit_level = row.getAs[String]("audit_level"),
        reconciliation_enabled = row.getAs[String]("reconciliation_enabled"),
        reconciliation_config = Option(row.getAs[String]("reconciliation_config")),
        dq_checks_enabled = row.getAs[String]("dq_checks_enabled"),
        dq_rules_config = Option(row.getAs[String]("dq_rules_config")),
        notification_emails_success = Option(row.getAs[String]("notification_emails_success")),
        notification_emails_failure = Option(row.getAs[String]("notification_emails_failure")),
        notification_verbosity = Option(row.getAs[String]("notification_verbosity")),
        log_masking_columns = Option(row.getAs[String]("log_masking_columns")),
        execution_authorization_flag = row.getAs[String]("execution_authorization_flag"),
        manual_trigger_only = row.getAs[String]("manual_trigger_only"),
        created_by = row.getAs[String]("created_by"),
        created_ts = row.getAs[java.sql.Timestamp]("created_ts"),
        updated_by = row.getAs[String]("updated_by"),
        updated_ts = row.getAs[java.sql.Timestamp]("updated_ts"),
        config_version = row.getAs[java.math.BigDecimal]("config_version").intValue()
      )

    } catch {
      case e: IllegalArgumentException => // Catch specific exception for missing Spark conf
        throw e
      case e: Exception =>
        val errorMsg = s"Failed to load configuration for job_id $jobId from Oracle: ${e.getMessage}"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }
  }
}
