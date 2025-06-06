package com.example.etl

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import com.example.etl.config.{ConfigLoader, JobConfig}
import com.example.etl.ingestion.handlers.{AsIsIngestionHandler, WithLogicIngestionHandler, Scd2LogicHandler}
import com.example.etl.audit.JobAuditor
import com.example.etl.validation.JobConfigValidator
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.nio.file.{Files, Paths}

object MainApp {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val AuditFilePathKey = "spark.etl.audit.filepath"
  val ExitCodeConfigValidationFailed = 5
  val ExitCodeJobInactive = 0

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("Usage: spark-submit <jar> com.example.etl.MainApp <job_name>")
      System.exit(1)
    }

    val jobName = args(0)
    logger.info(s"Attempting to start ETL job with Name: $jobName")

    var jobConfigOpt: Option[JobConfig] = None
    var recordsProcessedOpt: Option[Long] = None
    var recordsWrittenOpt: Option[Long] = None
    var sparkSessionInitialized = false
    var spark: SparkSession = null // Declare here to be in scope for finally

    try {
      spark = SparkSession.builder()
        .appName(s"ETL Framework Job: $jobName")
        .getOrCreate()
      sparkSessionInitialized = true
      logger.info(s"Spark Session created for job: $jobName. Spark version: ${spark.version}")

      val now = LocalDateTime.now()
      val year = now.format(DateTimeFormatter.ofPattern("yyyy"))
      val month = now.format(DateTimeFormatter.ofPattern("MM"))
      val timestampSuffix = now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS"))
      val defaultAuditFile = s"etl_job_audit_${jobName}_${timestampSuffix}.csv"
      val baseAuditLogDir = spark.conf.get("spark.etl.audit.basepath", "./audit_logs")
      val monthlyPartitionPath = Paths.get(baseAuditLogDir, year, month)
      val defaultAuditPath = monthlyPartitionPath.resolve(defaultAuditFile).toString
      val auditFilePath = spark.conf.get(AuditFilePathKey, defaultAuditPath)
      try {
        val finalAuditPathObj = Paths.get(auditFilePath)
        val finalAuditDir = finalAuditPathObj.getParent
        if (finalAuditDir != null && !Files.exists(finalAuditDir)) {
            Files.createDirectories(finalAuditDir)
            logger.info(s"Created audit log directory: ${finalAuditDir.toAbsolutePath.toString}")
        }
      } catch {
          case e: Exception => logger.warn(s"Could not create audit directory for path $auditFilePath: ${e.getMessage}", e)
      }
      JobAuditor.init(auditFilePath)

      val loadedJobConfig: JobConfig = ConfigLoader.loadConfig(jobName, spark)
      jobConfigOpt = Some(loadedJobConfig)
      logger.info(s"Successfully loaded configuration for job ${loadedJobConfig.job_name}")

      val validationErrors = JobConfigValidator.validate(loadedJobConfig, spark)
      if (validationErrors.nonEmpty) {
        logger.error(s"Job configuration validation failed for job '${loadedJobConfig.job_name}'. Errors found:")
        validationErrors.foreach(err => logger.error(s"  - $err"))
        JobAuditor.logJobFailure(loadedJobConfig, new IllegalArgumentException(s"Configuration validation failed: ${validationErrors.mkString("; ")}"))
        System.exit(ExitCodeConfigValidationFailed)
      }
      logger.info("Job configuration validation successful.")

      if (loadedJobConfig.is_active.toUpperCase != "Y") {
        logger.warn(s"Job ${loadedJobConfig.job_name} is marked as INACTIVE (is_active = '${loadedJobConfig.is_active}'). Skipping execution.")
        logger.info("Placeholder: Email Notification (Yellow) would be sent for inactive job.")
        JobAuditor.logJobSkipped(loadedJobConfig, s"Job marked as inactive (is_active='${loadedJobConfig.is_active}')") // Updated call
        System.exit(ExitCodeJobInactive)
      }

      JobAuditor.logJobStart(loadedJobConfig)

      logger.info(s"Transformation mode for job ${loadedJobConfig.job_name}: ${loadedJobConfig.transformation_mode}")

      val (processed, written) = loadedJobConfig.transformation_mode.toUpperCase match {
        case "AS_IS" =>
          AsIsIngestionHandler.execute(loadedJobConfig, spark)
        case "WITH_LOGIC" =>
          WithLogicIngestionHandler.execute(loadedJobConfig, spark)
        case "SCD2" =>
          Scd2LogicHandler.execute(loadedJobConfig, spark)
        case other =>
          val errMsg = s"Unsupported transformation_mode: '$other' for job: ${loadedJobConfig.job_name}"
          logger.error(errMsg)
          throw new IllegalArgumentException(errMsg)
      }
      recordsProcessedOpt = processed
      recordsWrittenOpt = written

      JobAuditor.logJobSuccess(loadedJobConfig, recordsProcessedOpt, recordsWrittenOpt)
      logger.info(s"Successfully completed ETL job: ${loadedJobConfig.job_name}")

    } catch {
      case e: Throwable =>
        logger.error(s"Critical Error running ETL job $jobName: ${e.getMessage}", e)
        jobConfigOpt match {
          case Some(jc) => JobAuditor.logJobFailure(jc, e, recordsProcessedOpt, recordsWrittenOpt)
          case None => logger.error(s"Job $jobName failed before JobConfig was loaded or available for JobAuditor.")
        }

        val exitCode = e match {
            case _: IllegalArgumentException if e.getMessage != null && e.getMessage.startsWith("Configuration validation failed") => ExitCodeConfigValidationFailed
            // Removed check for IllegalStateException for inactive job as it's now a clean exit before this catch block for that specific case
            case _: IllegalArgumentException => 2
            case _: UnsupportedOperationException => 4
            case _: RuntimeException => 3
            case _ => 1
        }
        System.exit(exitCode)
    } finally {
      if (sparkSessionInitialized && spark != null) {
        spark.stop()
        logger.info(s"Spark Session stopped for job: $jobName")
      } else {
        logger.info(s"Spark Session was not (fully) initialized for job: $jobName.")
      }
    }
  }
}
