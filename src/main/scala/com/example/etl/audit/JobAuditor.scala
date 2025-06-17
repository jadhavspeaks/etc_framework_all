package com.example.etl.audit

import com.example.etl.config.JobConfig
import com.example.etl.reconciliation.ReconciliationReport
import com.example.etl.dq.DataQualityReport // Import DQReport for potential future use in method signatures
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.util.Try
// Using a more robust JSON handling approach if possible, but sticking to basic for now if no new deps
import com.example.etl.util.JSONUtils // Assuming JSONUtils.escape is available and suitable

// Case classes remain the same...
case class AuditSummaryRecord(
  job_name: String,
  run_datetime: Timestamp,
  run_status: String,
  records_read: Option[Long] = None,
  records_written: Option[Long] = None,
  run_duration_ms: Option[Long] = None,
  error_message_short: Option[String] = None,
  audit_event_datetime: Timestamp,
  run_date: String,
  job_name_part: String
)

case class JobRunDetailRecord(
  job_name: String,
  run_datetime: Timestamp,
  spark_app_id: Option[String] = None,
  spark_tracking_url: Option[String] = None,
  job_config_json: Option[String] = None,
  full_stack_trace: Option[String] = None,
  reconciliation_report_json: Option[String] = None,
  dq_report_json: Option[String] = None,
  custom_job_properties_json: Option[String] = None,
  run_date: String,
  job_name_part: String
)

case class RejectionEventRecord(
  job_name: String,
  run_datetime: Timestamp,
  rejection_timestamp: Timestamp,
  stage: String,
  record_identifier: Option[String] = None,
  reason: String,
  rejected_data: Option[String] = None,
  rejection_date: String,
  job_name_part: String
)

object JobAuditor {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val AuditSummaryTableName = "etl_framework_audit.audit_summary"
  val JobRunDetailsTableName = "etl_framework_audit.job_run_details"
  val RejectionEventsTableName = "etl_framework_audit.rejection_events"
  private val DateFormatPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private def getCurrentAuditTimestamp: Timestamp = Timestamp.from(java.time.Instant.now())

  // Enhanced jobConfigToJson to include more fields, especially security flags
  private def jobConfigToJson(jobConfig: JobConfig): String = {
    // Helper to create JSON key-value pairs, handling Option types and escaping strings
    def kv[T](key: String, value: Option[T], isString: Boolean = true): String = {
      value.map(v => s"\"$key\": ${if(isString) s"\"${JSONUtils.escape(v.toString)}\"," else s"${v.toString},"}").getOrElse("")
    }
    def kvRequired(key: String, value: String, isString: Boolean = true): String = {
      s"\"$key\": ${if(isString) s"\"${JSONUtils.escape(value)}\"," else s"$value,"}"
    }

    val sb = new StringBuilder("{")
    sb.append(kvRequired("job_name", jobConfig.job_name))
    sb.append(kv("job_description", jobConfig.job_description))
    sb.append(kvRequired("is_active", jobConfig.is_active))
    sb.append(kvRequired("source_type", jobConfig.source_type))
    sb.append(kv("source_connection_details", jobConfig.source_connection_details))
    sb.append(kv("source_format", jobConfig.source_format))
    // CLOBs are large, consider omitting or truncating them in this summary JSON if too verbose for audit details
    // sb.append(kv("source_schema", jobConfig.source_schema.map(_.take(200)))) // Example truncation
    sb.append(kvRequired("target_type", jobConfig.target_type))
    sb.append(kv("target_connection_details", jobConfig.target_connection_details))
    sb.append(kvRequired("target_table_or_path", jobConfig.target_table_or_path))
    sb.append(kvRequired("load_type", jobConfig.load_type))
    sb.append(kvRequired("target_write_mode", jobConfig.target_write_mode))
    sb.append(kvRequired("transformation_mode", jobConfig.transformation_mode))
    // sb.append(kv("sql_logic", jobConfig.sql_logic.map(_.take(200)))) // Example truncation
    sb.append(kv("scd2_natural_keys", jobConfig.scd2_natural_keys))
    sb.append(kv("audit_level", Some(jobConfig.audit_level)))
    // Security flags
    sb.append(kvRequired("execution_authorization_flag", jobConfig.execution_authorization_flag))
    sb.append(kvRequired("manual_trigger_only", jobConfig.manual_trigger_only))
    // Remove trailing comma if any and close JSON object
    if (sb.length > 1 && sb.charAt(sb.length - 1) == ',') {
      sb.deleteCharAt(sb.length - 1)
    }
    sb.append("}")
    sb.toString
  }

  private def reconReportToJson(report: ReconciliationReport): String = report.toJsonString // Assuming it exists
  private def dqReportToJson(report: DataQualityReport): String = report.toJsonString // Assuming it exists

  private def writeToHive(df: DataFrame, tableName: String, spark: SparkSession): Unit = {
    Try {
      // Ensure dynamic partitioning is enabled if writing to partitioned Hive tables and Spark version requires it explicitly set here
      // spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
      // spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      df.write.mode(SaveMode.Append).insertInto(tableName)
      logger.info(s"Wrote ${Try(df.count().toString).getOrElse("some")} records to Hive table: $tableName")
    } match { case Failure(e) => logger.error(s"Failed to write to Hive $tableName: ${e.getMessage}", e) case Success(_) => }
  }

  def logJobStart(jobConfig: JobConfig, startTimeMillis: Long, spark: SparkSession): Unit = {
    val runDT = new Timestamp(startTimeMillis); val runDStr = runDT.toLocalDateTime.format(DateFormatPattern); val jnp = jobConfig.job_name; val eventTs = getCurrentAuditTimestamp
    val sumRec = AuditSummaryRecord(jnp,runDT,"STARTED",audit_event_datetime=eventTs,run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(sumRec)), AuditSummaryTableName, spark)
    val detRec = JobRunDetailRecord(jnp,runDT,Some(spark.sparkContext.applicationId),job_config_json=Some(jobConfigToJson(jobConfig)),custom_job_properties_json=jobConfig.job_properties,run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(detRec)), JobRunDetailsTableName, spark)
    logger.info(s"Logged START for job ${jobConfig.job_name} to Hive.")
  }

  def logJobSuccess(jobConfig: JobConfig, rRead: Option[Long], rWritten: Option[Long], startMs: Long, endMs: Long, reconReportOpt: Option[ReconciliationReport], dqReportOpt: Option[DataQualityReport], spark: SparkSession): Unit = {
    val runDT=new Timestamp(startMs); val runDStr=runDT.toLocalDateTime.format(DateFormatPattern); val jnp=jobConfig.job_name; val eventTs=getCurrentAuditTimestamp; val dur=endMs-startMs
    val sumRec=AuditSummaryRecord(jnp,runDT,"SUCCESS",rRead,rWritten,Some(dur),audit_event_datetime=eventTs,run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(sumRec)), AuditSummaryTableName, spark)
    // Pass the full jobConfig to jobConfigToJson for the detail record on success
    val detRec=JobRunDetailRecord(jnp,runDT,Some(spark.sparkContext.applicationId),reconciliation_report_json=reconReportOpt.map(reconReportToJson), dq_report_json=dqReportOpt.map(dqReportToJson), job_config_json=Some(jobConfigToJson(jobConfig)), custom_job_properties_json=jobConfig.job_properties, run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(detRec)), JobRunDetailsTableName, spark)
    logger.info(s"Logged SUCCESS for job ${jobConfig.job_name} to Hive. Duration: $dur ms.")
  }

  def logJobFailure(jobConfigOpt: Option[JobConfig], jName: String, err: Throwable, rRead: Option[Long], rWritten: Option[Long], startMs: Long, endMs: Long, dqReportOpt: Option[DataQualityReport], spark: SparkSession): Unit = {
    val runDT=new Timestamp(startMs); val runDStr=runDT.toLocalDateTime.format(DateFormatPattern); val jnp=jName; val eventTs=getCurrentAuditTimestamp; val dur=endMs-startMs
    val shortErr=s"${err.getClass.getName}: ${err.getMessage}".take(1000); val stack=Try(new java.io.StringWriter()).flatMap(sw=>Try(err.printStackTrace(new java.io.PrintWriter(sw))).map(_=>sw.toString)).getOrElse("No stack trace.")
    val sumRec=AuditSummaryRecord(jnp,runDT,"FAILED",rRead,rWritten,Some(dur),Some(shortErr),audit_event_datetime=eventTs,run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(sumRec)), AuditSummaryTableName, spark)
    val detRec=JobRunDetailRecord(jnp,runDT,Try(spark.sparkContext.applicationId).toOption,job_config_json=jobConfigOpt.map(jobConfigToJson),custom_job_properties_json=jobConfigOpt.flatMap(_.job_properties),full_stack_trace=Some(stack), dq_report_json=dqReportOpt.map(dqReportToJson), run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(detRec)), JobRunDetailsTableName, spark)
    logger.info(s"Logged FAILED for job $jName to Hive. Duration: $dur ms.")
  }

  def logJobSkipped(jobConfig: JobConfig, skipReason: String, spark: SparkSession): Unit = {
    val runDT=getCurrentAuditTimestamp; val runDStr=runDT.toLocalDateTime.format(DateFormatPattern); val jnp=jobConfig.job_name
    val sumRec=AuditSummaryRecord(jnp,runDT,"SKIPPED_INACTIVE",error_message_short=Some(skipReason),audit_event_datetime=runDT,run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(sumRec)), AuditSummaryTableName, spark)
    // Pass the full jobConfig to jobConfigToJson for the detail record on skip
    val detRec=JobRunDetailRecord(jnp,runDT,job_config_json=Some(jobConfigToJson(jobConfig)),custom_job_properties_json=jobConfig.job_properties,full_stack_trace=Some(skipReason),run_date=runDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(detRec)), JobRunDetailsTableName, spark)
    logger.info(s"Logged SKIPPED_INACTIVE for job ${jobConfig.job_name} to Hive. Reason: $skipReason")
  }

  def logRejection(jobConfig: JobConfig, stage: String, reason: String, runDateTime: Timestamp, recordId: Option[String]=None, rejectedData: Option[String]=None, spark: SparkSession): Unit = {
    val rejTs=getCurrentAuditTimestamp; val rejDStr=rejTs.toLocalDateTime.format(DateFormatPattern); val jnp=jobConfig.job_name
    val rec=RejectionEventRecord(jnp,runDateTime,rejTs,stage,recordId,reason,rejectedData,rejection_date=rejDStr,job_name_part=jnp)
    writeToHive(spark.createDataFrame(Seq(rec)), RejectionEventsTableName, spark)
    logger.info(s"Rejection logged for job ${jobConfig.job_name}, Stage: $stage. Written to Hive.")
  }
}
