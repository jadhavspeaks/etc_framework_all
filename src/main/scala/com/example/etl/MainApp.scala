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

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("Usage: spark-submit <jar> com.example.etl.MainApp <job_id> [other_args...]")
      System.exit(1)
    }

    val jobId = args(0)
    logger.info(s"Starting ETL job with ID: $jobId")

    val spark = SparkSession.builder()
      .appName(s"ETL Framework Job: $jobId")
      .getOrCreate()

    // Configure and initialize JobAuditor with monthly partitioned paths
    val now = LocalDateTime.now()
    val year = now.format(DateTimeFormatter.ofPattern("yyyy"))
    val month = now.format(DateTimeFormatter.ofPattern("MM"))
    val timestampSuffix = now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS"))

    val defaultAuditFile = s"etl_job_audit_${jobId}_${timestampSuffix}.csv"
    // Base directory for all audit logs, could be configurable too
    val baseAuditLogDir = spark.conf.get("spark.etl.audit.basepath", "./audit_logs")
    val monthlyPartitionPath = Paths.get(baseAuditLogDir, year, month)
    val defaultAuditPath = monthlyPartitionPath.resolve(defaultAuditFile).toString

    val auditFilePath = spark.conf.get(AuditFilePathKey, defaultAuditPath)

    // Ensure the specific directory for the audit file (including YYYY/MM) exists
    // JobAuditor.init will also attempt to create parent dirs, but doing it here for the base path is good too.
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

    var jobConfigOpt: Option[JobConfig] = None
    var recordsProcessedOpt: Option[Long] = None
    var recordsWrittenOpt: Option[Long] = None

    try {
      logger.info(s"Spark Session created for job: $jobId. Spark version: ${spark.version}")

      val loadedJobConfig: JobConfig = ConfigLoader.loadConfig(jobId, spark)
      jobConfigOpt = Some(loadedJobConfig)
      logger.info(s"Successfully loaded configuration for job $jobId: ${loadedJobConfig.job_name}")

      val validationErrors = JobConfigValidator.validate(loadedJobConfig, spark)
      if (validationErrors.nonEmpty) {
        logger.error(s"Job configuration validation failed for job_id '$jobId'. Errors found:")
        validationErrors.foreach(err => logger.error(s"  - $err"))
        JobAuditor.logJobFailure(loadedJobConfig, new IllegalArgumentException(s"Configuration validation failed: ${validationErrors.mkString("; ")}"))
        System.exit(ExitCodeConfigValidationFailed)
      }
      logger.info("Job configuration validation successful.")

      JobAuditor.logJobStart(loadedJobConfig)

      logger.info(s"Transformation mode for job $jobId: ${loadedJobConfig.transformation_mode}")

      val (processed, written) = loadedJobConfig.transformation_mode.toUpperCase match {
        case "AS_IS" =>
          AsIsIngestionHandler.execute(loadedJobConfig, spark)
        case "WITH_LOGIC" =>
          logger.warn("WITH_LOGIC mode handler called. Ensure it returns (Option[Long], Option[Long]) for record counts.")
          WithLogicIngestionHandler.execute(loadedJobConfig, spark)
        case "SCD2" =>
          Scd2LogicHandler.execute(loadedJobConfig, spark)
        case other =>
          val errMsg = s"Unsupported transformation_mode: '$other' for job_id: ${loadedJobConfig.job_id}"
          logger.error(errMsg)
          throw new IllegalArgumentException(errMsg)
      }
      recordsProcessedOpt = processed
      recordsWrittenOpt = written

      JobAuditor.logJobSuccess(loadedJobConfig, recordsProcessedOpt, recordsWrittenOpt)
      logger.info(s"Successfully completed ETL job: $jobId")

    } catch {
      case e: Throwable =>
        logger.error(s"Critical Error running ETL job $jobId: ${e.getMessage}", e)
        jobConfigOpt match {
          case Some(jc) => JobAuditor.logJobFailure(jc, e, recordsProcessedOpt, recordsWrittenOpt)
          case None => logger.error(s"Job $jobId failed before configuration could be loaded or was available for audit logging.")
        }

        val exitCode = e match {
            case _: IllegalArgumentException if e.getMessage != null && e.getMessage.startsWith("Configuration validation failed") => ExitCodeConfigValidationFailed
            case _: IllegalArgumentException => 2
            case _: UnsupportedOperationException => 4
            case _: RuntimeException => 3
            case _ => 1
        }
        System.exit(exitCode)
    } finally {
      spark.stop()
      logger.info(s"Spark Session stopped for job: $jobId")
    }
  }
}
