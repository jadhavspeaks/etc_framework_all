package com.example.etl

import com.example.etl.audit.JobAuditor
import com.example.etl.config.{JobConfig, ConfigLoader}
import com.example.etl.dq.DataQualityReport // Ensure DQReport is imported
import com.example.etl.ingestion.handlers.{AsIsIngestionHandler, Scd2LogicHandler, WithLogicIngestionHandler}
import com.example.etl.reconciliation.ReconciliationReport
import com.example.etl.validation.JobConfigValidator
import com.example.etl.notification.NotificationService // Import NotificationService
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import scala.util.Try

object MainApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val ExitCodeConfigValidationFailed = 5
  val ExitCodeJobInactive = 0
  val ExitCodeDQChecksFailed = 6

  private def buildMetricsMap(
    jobConfig: JobConfig,
    status: String,
    startTimeMillis: Long,
    endTimeMillis: Long,
    recordsProcessed: Option[Long],
    recordsWritten: Option[Long],
    sparkAppId: Option[String],
    dqReportOpt: Option[DataQualityReport],
    reconReportOpt: Option[ReconciliationReport],
    error: Option[Throwable] = None,
    skipReason: Option[String] = None
  ): Map[String, String] = {
    val baseMetrics = scala.collection.mutable.Map[String, String]()
    baseMetrics += ("JOB_NAME" -> jobConfig.job_name)
    baseMetrics += ("RUN_DATETIME" -> new Timestamp(startTimeMillis).toLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    baseMetrics += ("STATUS" -> status)
    baseMetrics += ("DURATION_MS" -> (endTimeMillis - startTimeMillis).toString)
    recordsProcessed.foreach(c => baseMetrics += ("RECORDS_READ" -> c.toString))
    recordsWritten.foreach(c => baseMetrics += ("RECORDS_WRITTEN" -> c.toString))
    sparkAppId.foreach(id => baseMetrics += ("SPARK_APP_ID" -> id))
    dqReportOpt.foreach(dq => baseMetrics += ("DQ_REPORT_STATUS" -> dq.overall_status))
    reconReportOpt.foreach(rc => baseMetrics += ("RECON_REPORT_STATUS" -> rc.overallStatus))
    error.foreach { e =>
      baseMetrics += ("ERROR_TYPE" -> e.getClass.getName)
      baseMetrics += ("ERROR_MESSAGE_SHORT" -> e.getMessage.take(200)) // Short version for email
    }
    skipReason.foreach(sr => baseMetrics += ("SKIP_REASON" -> sr))
    baseMetrics.toMap
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) { logger.error("Usage: <job_name>"); System.exit(1) }
    val jobName = args(0)
    logger.info(s"Attempting to start ETL job: $jobName")

    var jobConfigOpt: Option[JobConfig] = None
    var recordsProcessedOpt: Option[Long] = None
    var recordsWrittenOpt: Option[Long] = None
    var dataQualityReportOpt: Option[DataQualityReport] = None
    var reconciliationReportOpt: Option[ReconciliationReport] = None // Still placeholder
    var spark: SparkSession = null
    var sparkSessionInitialized = false
    var startTimeMillis: Long = System.currentTimeMillis() // Initialize early for failure scenarios

    Try {
      spark = SparkSession.builder().appName(s"ETL Job: $jobName").enableHiveSupport().getOrCreate()
      sparkSessionInitialized = true
      startTimeMillis = System.currentTimeMillis() // More accurate start time
      val sparkAppId = Try(spark.sparkContext.applicationId).toOption
      logger.info(s"Spark Session created for $jobName. AppID: ${sparkAppId.getOrElse("N/A")}. Start: $startTimeMillis")

      val loadedJobConfig = ConfigLoader.loadConfig(jobName, spark)
      jobConfigOpt = Some(loadedJobConfig)
      logger.info(s"Loaded config for ${loadedJobConfig.job_name}")
      JobAuditor.logJobStart(loadedJobConfig, startTimeMillis, spark)

      val validationErrors = JobConfigValidator.validate(loadedJobConfig, spark)
      if (validationErrors.nonEmpty) {
        val errorMsg = s"Config validation failed: ${validationErrors.mkString("; ")}"
        logger.error(s"Job '${loadedJobConfig.job_name}' validation errors: ${errorMsg}")
        throw new IllegalArgumentException(errorMsg) // Caught by main catch, JobAuditor.logFailure then called
      }
      logger.info("Config validation successful.")

      if (loadedJobConfig.is_active.toUpperCase != "Y") {
        val skipMsg = s"Job marked as inactive (is_active='${loadedJobConfig.is_active}')"
        logger.warn(s"Job ${loadedJobConfig.job_name} is INACTIVE. $skipMsg. Skipping execution.")
        JobAuditor.logJobSkipped(loadedJobConfig, skipMsg, spark)
        val metrics = buildMetricsMap(loadedJobConfig, "SKIPPED_INACTIVE", startTimeMillis, System.currentTimeMillis(), None,None,sparkAppId,None,None,skipReason=Some(skipMsg))
        Try(NotificationService.sendNotification(loadedJobConfig, "SKIPPED_INACTIVE", metrics, spark))
          .recover{ case e => logger.error(s"Failed to send SKIPPED_INACTIVE notification for ${loadedJobConfig.job_name}: ${e.getMessage}", e)}
        System.exit(ExitCodeJobInactive)
      }

      val (processed, written, dqReport) = loadedJobConfig.transformation_mode.toUpperCase match {
        case "AS_IS" => AsIsIngestionHandler.execute(loadedJobConfig, spark)
        case "WITH_LOGIC" => WithLogicIngestionHandler.execute(loadedJobConfig, spark)
        case "SCD2" => Scd2LogicHandler.execute(loadedJobConfig, spark)
        case other => throw new IllegalArgumentException(s"Unsupported transformation_mode: '$other'")
      }
      recordsProcessedOpt = processed; recordsWrittenOpt = written; dataQualityReportOpt = dqReport

      // Placeholder: Reconciliation would occur here or be returned by handlers to populate reconciliationReportOpt
      // For now, reconciliation logging is within handlers.

      val endTimeMillis = System.currentTimeMillis()
      JobAuditor.logJobSuccess(loadedJobConfig, recordsProcessedOpt, recordsWrittenOpt, startTimeMillis, endTimeMillis, reconciliationReportOpt, dataQualityReportOpt, spark)
      logger.info(s"Successfully completed ETL job: ${loadedJobConfig.job_name}")
      val successMetrics = buildMetricsMap(loadedJobConfig, "SUCCESS", startTimeMillis, endTimeMillis, recordsProcessedOpt, recordsWrittenOpt, sparkAppId, dataQualityReportOpt, reconciliationReportOpt)
      Try(NotificationService.sendNotification(loadedJobConfig, "SUCCESS", successMetrics, spark))
        .recover{ case e => logger.error(s"Failed to send SUCCESS notification for ${loadedJobConfig.job_name}: ${e.getMessage}", e)}

    } match {
      case Success(_) => // Normal exit handled by System.exit for inactive or by reaching end for success
      case Failure(e) =>
        val endTimeMillisOnError = System.currentTimeMillis()
        val sparkAppIdOnError = if(sparkSessionInitialized && spark != null) Try(spark.sparkContext.applicationId).toOption else None
        logger.error(s"Critical Error running ETL job $jobName: ${e.getMessage}", e)
        // Ensure JobAuditor.logJobFailure is called before attempting to send notification
        JobAuditor.logJobFailure(jobConfigOpt, jobName, e, recordsProcessedOpt, recordsWrittenOpt, startTimeMillis, endTimeMillisOnError, dataQualityReportOpt, spark)

        jobConfigOpt.foreach { jc => // Send notification only if config was loaded
            val failureMetrics = buildMetricsMap(jc, "FAILED", startTimeMillis, endTimeMillisOnError, recordsProcessedOpt, recordsWrittenOpt, sparkAppIdOnError, dataQualityReportOpt, reconciliationReportOpt, error = Some(e))
            Try(NotificationService.sendNotification(jc, "FAILED", failureMetrics, spark))
              .recover{ case mailEx => logger.error(s"Failed to send FAILED notification for $jobName: ${mailEx.getMessage}", mailEx)}
        }

        val exitCode = e match {
            case _: IllegalArgumentException if e.getMessage != null && e.getMessage.startsWith("Config validation failed") => ExitCodeConfigValidationFailed
            case _: RuntimeException if e.getMessage != null && e.getMessage.startsWith("Critical DQ checks failed") => ExitCodeDQChecksFailed
            case _: IllegalArgumentException => 2
            case _: UnsupportedOperationException => 4
            case _: RuntimeException => 3
            case _ => 1
        }
        System.exit(exitCode)
    } finally {
      if (sparkSessionInitialized && spark != null) { spark.stop(); logger.info(s"Spark Session stopped for $jobName") }
      else { logger.info(s"Spark Session not fully initialized or already stopped for $jobName.") }
    }
  }
}
