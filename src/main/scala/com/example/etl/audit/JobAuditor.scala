package com.example.etl.audit

import com.example.etl.config.JobConfig
import com.example.etl.reconciliation.ReconciliationReport // For logging reconciliation results
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.util.parsing.json.JSON // For serializing complex objects to JSON strings for details table

// --- Case Classes for Audit Data ---
case class AuditSummaryRecord(
  job_name: String,
  run_datetime: Timestamp,
  run_status: String, // e.g., STARTED, SUCCESS, FAILED, SKIPPED_INACTIVE
  records_read: Option[Long] = None,
  records_written: Option[Long] = None,
  run_duration_ms: Option[Long] = None,
  error_message_short: Option[String] = None,
  audit_event_datetime: Timestamp,
  // Partitioning columns - derived from run_datetime and job_name
  run_date: String, // YYYY-MM-DD
  job_name_part: String // job_name, for partitioning convenience
)

case class JobRunDetailRecord(
  job_name: String,
  run_datetime: Timestamp, // Links to AuditSummaryTable
  spark_app_id: Option[String] = None,
  spark_tracking_url: Option[String] = None, // Placeholder
  job_config_json: Option[String] = None,
  full_stack_trace: Option[String] = None,
  reconciliation_report_json: Option[String] = None,
  dq_report_json: Option[String] = None, // For future DQ integration
  custom_job_properties_json: Option[String] = None,
  // Partitioning columns
  run_date: String, // YYYY-MM-DD
  job_name_part: String
)

case class RejectionEventRecord(
  job_name: String,
  run_datetime: Timestamp, // Timestamp of the job run that generated this rejection
  rejection_timestamp: Timestamp,
  stage: String,
  record_identifier: Option[String] = None,
  reason: String,
  rejected_data: Option[String] = None,
  // Partitioning columns
  rejection_date: String, // YYYY-MM-DD
  job_name_part: String
)

object JobAuditor {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  // Hive table names (assuming an 'etl_framework_audit' database exists or is created by user)
  val AuditSummaryTableName = "etl_framework_audit.audit_summary"
  val JobRunDetailsTableName = "etl_framework_audit.job_run_details"
  val RejectionEventsTableName = "etl_framework_audit.rejection_events"

  private val DateFormatPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  // Helper to get current timestamp for audit records
  private def getCurrentAuditTimestamp: Timestamp = Timestamp.from(java.time.Instant.now())

  // Helper to stringify JobConfig (simple JSON for now, could use a library)
  private def jobConfigToJson(jobConfig: JobConfig): String = {
    // Basic manual JSON construction; a proper library (e.g. Circe, Jackson) would be better for complex objects or production
    // This is a simplified version. For a real implementation, use a JSON library to handle all fields.
    s"{\"job_name\": \"${jobConfig.job_name}\", \"source_type\": \"${jobConfig.source_type}\", \"target_type\": \"${jobConfig.target_type}\", \"transformation_mode\": \"${jobConfig.transformation_mode}\"}" // etc.
  }

  // Helper to stringify ReconciliationReport
  private def reconReportToJson(report: ReconciliationReport): String = {
      // This is a simplified version. For a real implementation, use a JSON library.
      val checksJson = report.checkResults.map(cr => s"{\"checkType\":\"${cr.checkType}\", \"status\":\"${cr.status}\"}").mkString("[",",","]")
      s"{\"jobName\":\"${report.jobName}\", \"overallStatus\":\"${report.overallStatus}\", \"checkResults\": $checksJson }"
  }

  // Helper to write DataFrame to a Hive table with dynamic partitioning
  private def writeToHive(df: DataFrame, tableName: String, spark: SparkSession): Unit = {
    Try {
      import spark.implicits._ // Needed for .as and .toDF if not already in scope
      // Ensure Hive support is enabled in SparkSession if built without it by default
      // spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict") // Often needed
      df.write.mode(SaveMode.Append).insertInto(tableName) // Assumes table exists and is partitioned
      logger.info(s"Successfully wrote ${Try(df.count()).getOrElse("some")} records to Hive table: $tableName") // Added Try for count
    } match {
      case Failure(e) => logger.error(s"Failed to write to Hive table $tableName: ${e.getMessage}", e)
      case Success(_) =>
    }
  }

  // No specific init needed for file paths anymore. Hive tables are assumed to exist.
  // SparkSession is now passed to each logging method.

  def logJobStart(jobConfig: JobConfig, startTimeMillis: Long, spark: SparkSession): Unit = {
    val runDateTime = new Timestamp(startTimeMillis)
    val runDateStr = runDateTime.toLocalDateTime.format(DateFormatPattern)
    val jobNamePart = jobConfig.job_name
    val eventTimestamp = getCurrentAuditTimestamp

    val summaryRecord = AuditSummaryRecord(
      job_name = jobConfig.job_name,
      run_datetime = runDateTime,
      run_status = "STARTED",
      audit_event_datetime = eventTimestamp,
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val summaryDF = spark.createDataFrame(Seq(summaryRecord))
    writeToHive(summaryDF, AuditSummaryTableName, spark)

    val detailRecord = JobRunDetailRecord(
      job_name = jobConfig.job_name,
      run_datetime = runDateTime,
      spark_app_id = Some(spark.sparkContext.applicationId),
      // spark_tracking_url = Some(spark.sparkContext.uiWebUrl.getOrElse("N/A")), // uiWebUrl might not always be available
      job_config_json = Some(jobConfigToJson(jobConfig)),
      custom_job_properties_json = jobConfig.job_properties,
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val detailDF = spark.createDataFrame(Seq(detailRecord))
    writeToHive(detailDF, JobRunDetailsTableName, spark)
    logger.info(s"Logged START for job ${jobConfig.job_name} to Hive.")
  }

  def logJobSuccess(
    jobConfig: JobConfig,
    recordsRead: Option[Long],
    recordsWritten: Option[Long],
    startTimeMillis: Long,
    endTimeMillis: Long,
    reconciliationReport: Option[ReconciliationReport], // Added for details table
    spark: SparkSession
  ): Unit = {
    val runDateTime = new Timestamp(startTimeMillis)
    val runDateStr = runDateTime.toLocalDateTime.format(DateFormatPattern)
    val jobNamePart = jobConfig.job_name
    val eventTimestamp = getCurrentAuditTimestamp
    val duration = endTimeMillis - startTimeMillis

    val summaryRecord = AuditSummaryRecord(
      job_name = jobConfig.job_name,
      run_datetime = runDateTime,
      run_status = "SUCCESS",
      records_read = recordsRead,
      records_written = recordsWritten,
      run_duration_ms = Some(duration),
      audit_event_datetime = eventTimestamp,
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val summaryDF = spark.createDataFrame(Seq(summaryRecord))
    writeToHive(summaryDF, AuditSummaryTableName, spark)

    // Update JobRunDetailsTable with success info (e.g., reconciliation report)
    val detailRecord = JobRunDetailRecord(
      job_name = jobConfig.job_name,
      run_datetime = runDateTime,
      spark_app_id = Some(spark.sparkContext.applicationId),
      reconciliation_report_json = reconciliationReport.map(reconReportToJson),
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val detailDF = spark.createDataFrame(Seq(detailRecord))
    writeToHive(detailDF, JobRunDetailsTableName, spark) // This will append; ideally, we'd update if an entry for this run_datetime exists.
                                                       // For simplicity, append; queries can pick latest based on audit_event_datetime for a run_datetime.
    logger.info(s"Logged SUCCESS for job ${jobConfig.job_name} to Hive. Duration: $duration ms.")
  }

  def logJobFailure(
    jobConfigOpt: Option[JobConfig], // JobConfig might not be available if failure is early
    jobName: String, // Always available from MainApp args
    error: Throwable,
    recordsRead: Option[Long],
    recordsWritten: Option[Long],
    startTimeMillis: Long,
    endTimeMillis: Long,
    spark: SparkSession
  ): Unit = {
    val runDateTime = new Timestamp(startTimeMillis)
    val runDateStr = runDateTime.toLocalDateTime.format(DateFormatPattern)
    val jobNamePart = jobName
    val eventTimestamp = getCurrentAuditTimestamp
    val duration = endTimeMillis - startTimeMillis

    val shortError = s"${error.getClass.getName}: ${error.getMessage}".take(1000) // Truncate for summary table
    val fullStackTrace = Try(new java.io.StringWriter()).flatMap(sw => Try(error.printStackTrace(new java.io.PrintWriter(sw))) .map(_ => sw.toString)).getOrElse("Could not get stack trace.")

    val summaryRecord = AuditSummaryRecord(
      job_name = jobName,
      run_datetime = runDateTime,
      run_status = "FAILED",
      records_read = recordsRead,
      records_written = recordsWritten,
      run_duration_ms = Some(duration),
      error_message_short = Some(shortError),
      audit_event_datetime = eventTimestamp,
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val summaryDF = spark.createDataFrame(Seq(summaryRecord))
    writeToHive(summaryDF, AuditSummaryTableName, spark)

    val detailRecord = JobRunDetailRecord(
      job_name = jobName,
      run_datetime = runDateTime,
      spark_app_id = Try(spark.sparkContext.applicationId).toOption, // Spark context might not be available if failure is very early
      job_config_json = jobConfigOpt.map(jobConfigToJson),
      custom_job_properties_json = jobConfigOpt.flatMap(_.job_properties),
      full_stack_trace = Some(fullStackTrace),
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val detailDF = spark.createDataFrame(Seq(detailRecord))
    writeToHive(detailDF, JobRunDetailsTableName, spark)
    logger.info(s"Logged FAILED for job $jobName to Hive. Duration: $duration ms.")
  }

  def logJobSkipped(jobConfig: JobConfig, skipReason: String, spark: SparkSession): Unit = {
    val runDateTime = getCurrentAuditTimestamp // Use current time as effective run time for skip
    val runDateStr = runDateTime.toLocalDateTime.format(DateFormatPattern)
    val jobNamePart = jobConfig.job_name

    val summaryRecord = AuditSummaryRecord(
      job_name = jobConfig.job_name,
      run_datetime = runDateTime,
      run_status = "SKIPPED_INACTIVE",
      error_message_short = Some(skipReason),
      audit_event_datetime = runDateTime,
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val summaryDF = spark.createDataFrame(Seq(summaryRecord))
    writeToHive(summaryDF, AuditSummaryTableName, spark)

    // Optionally, log to JobRunDetailsTable as well
    val detailRecord = JobRunDetailRecord(
      job_name = jobConfig.job_name,
      run_datetime = runDateTime,
      job_config_json = Some(jobConfigToJson(jobConfig)),
      custom_job_properties_json = jobConfig.job_properties,
      full_stack_trace = Some(skipReason), // Put skip reason in stack trace for detail
      run_date = runDateStr,
      job_name_part = jobNamePart
    )
    val detailDF = spark.createDataFrame(Seq(detailRecord))
    writeToHive(detailDF, JobRunDetailsTableName, spark)
    logger.info(s"Logged SKIPPED_INACTIVE for job ${jobConfig.job_name} to Hive. Reason: $skipReason")
  }

  def logRejection(
    jobConfig: JobConfig,
    stage: String,
    reason: String,
    runDateTime: Timestamp, // Pass the job's run_datetime for correlation
    recordIdentifier: Option[String] = None,
    rejectedData: Option[String] = None,
    spark: SparkSession
  ): Unit = {
    val rejectionTimestamp = getCurrentAuditTimestamp
    val rejectionDateStr = rejectionTimestamp.toLocalDateTime.format(DateFormatPattern)
    val jobNamePart = jobConfig.job_name

    val record = RejectionEventRecord(
      job_name = jobConfig.job_name,
      run_datetime = runDateTime,
      rejection_timestamp = rejectionTimestamp,
      stage = stage,
      record_identifier = recordIdentifier,
      reason = reason,
      rejected_data = rejectedData,
      rejection_date = rejectionDateStr,
      job_name_part = jobNamePart
    )
    val rejectionDF = spark.createDataFrame(Seq(record))
    writeToHive(rejectionDF, RejectionEventsTableName, spark)
    logger.info(s"Rejection logged for job ${jobConfig.job_name}, Stage: $stage, Reason: $reason. Written to Hive.")
  }
}
