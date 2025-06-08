package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.sql.{SqlParserService, LogicalPlanValidator}
import com.example.etl.reconciliation.ReconciliationService
import com.example.etl.dq.{DataQualityService, DataQualityReport} // Import DQ types
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode, Dataset}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON

object WithLogicIngestionHandler {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long], Option[DataQualityReport]) = { // Updated signature
    logger.info(s"Executing WITH_LOGIC ingestion for job_name: ${jobConfig.job_name}")
    var recordsReadCount: Option[Long] = None
    var recordsWrittenCount: Option[Long] = None
    var dataQualityReportOpt: Option[DataQualityReport] = None // For DQ results

    var sourceDF: DataFrame = null
    jobConfig.source_type.toUpperCase match {
      case "FILE" =>
        if (jobConfig.source_connection_details.isEmpty) throw new IllegalArgumentException(s"Source path required for FILE source in ${jobConfig.job_name}")
        val sourcePath = jobConfig.source_connection_details.get
        val format = jobConfig.source_format.getOrElse(throw new IllegalArgumentException("source_format required for FILE source")).toLowerCase
        val reader = spark.read.format(format)
        jobConfig.source_format_options.foreach { o => JSON.parseFull(o) match {case Some(m:Map[String,String])=>reader.options(m) case _=>logger.warn(s"Could not parse source_opts: $o")} }
        jobConfig.source_schema.foreach{ s => reader.schema(s) }
        sourceDF = reader.load(sourcePath)
      case "HIVE_TABLE" =>
        if (jobConfig.source_connection_details.isEmpty) throw new IllegalArgumentException(s"Source table name required for HIVE_TABLE source in ${jobConfig.job_name}")
        val tableName = jobConfig.source_connection_details.get
        try { sourceDF = spark.table(tableName) } catch { case e: Exception => throw new RuntimeException(s"Failed to read HIVE_TABLE ${tableName}: ${e.getMessage}", e)}
      case other => throw new UnsupportedOperationException(s"Source type '$other' not supported for WITH_LOGIC.")
    }

    if (sourceDF == null) throw new RuntimeException(s"Source DF could not be loaded for ${jobConfig.job_name}.")
    if (jobConfig.audit_level.toUpperCase != "NONE") { try { recordsReadCount = Some(sourceDF.count()) } catch { case e: Exception => logger.warn(s"Could not count source records for ${jobConfig.job_name}: ${e.getMessage}", e) } }
    logger.info(s"Source data read for ${jobConfig.job_name}. Records: ${recordsReadCount.getOrElse("N/A")}")
    if (logger.isDebugEnabled) sourceDF.printSchema()

    val sourceViewName = jobConfig.job_name + "_source_input_for_logic"
    sourceDF.createOrReplaceTempView(sourceViewName)
    logger.info(s"Registered source DF as temp view: '$sourceViewName' for ${jobConfig.job_name}.")

    val sqlLogic = jobConfig.sql_logic.getOrElse(throw new IllegalArgumentException(s"sql_logic required for ${jobConfig.job_name}"))
    if (sqlLogic.trim.isEmpty) throw new IllegalArgumentException(s"sql_logic cannot be empty for ${jobConfig.job_name}")

    // Corrected LogicalPlan validation to handle Either properly
    val logicalPlan = SqlParserService.parse(sqlLogic, spark) match {
        case Right(plan) => logger.info(s"SQL parsed for ${jobConfig.job_name}."); plan
        case Left(errorMsg) => throw new RuntimeException(s"SQL parsing failed for ${jobConfig.job_name}: $errorMsg")
    }
    LogicalPlanValidator.validate(logicalPlan) match {
        case Right(_) => logger.info(s"LogicalPlan valid for ${jobConfig.job_name}.")
        case Left(errors) => throw new RuntimeException(s"LogicalPlan invalid for ${jobConfig.job_name}: ${errors.mkString("; ")}")
    }

    val transformedDF = try { Dataset.ofRows(spark, logicalPlan) } catch { case e: Exception => throw new RuntimeException(s"Error from LogicalPlan for ${jobConfig.job_name}: ${e.getMessage}", e)}
    if (logger.isDebugEnabled) { logger.debug(s"Schema after SQL logic for ${jobConfig.job_name}:"); transformedDF.printSchema(); transformedDF.show(5,truncate=false); }

    val writer = transformedDF.write
    jobConfig.target_format.foreach(f => writer.format(f.toLowerCase))
    jobConfig.target_format_options.foreach { o => JSON.parseFull(o) match {case Some(m:Map[String,String])=>writer.options(m) case _ => }}
    jobConfig.partitioning_columns.foreach { pc => val c=pc.split(',').map(_.trim).filter(_.nonEmpty); if(c.nonEmpty) writer.partitionBy(c:_*) }
    val sm = jobConfig.target_write_mode.toLowerCase match { case "overwrite"=>SaveMode.Overwrite case "append"=>SaveMode.Append case "ignore"=>SaveMode.Ignore case "errorifexists"|"error"=>SaveMode.ErrorIfExists case _=>SaveMode.Overwrite }
    writer.mode(sm)

    jobConfig.target_type.toUpperCase match {
      case "HDFS" | "FILE" => writer.save(jobConfig.target_table_or_path)
      case "HIVE_TABLE" => writer.saveAsTable(jobConfig.target_table_or_path)
      case other => throw new UnsupportedOperationException(s"Target type '$other' not supported by WITH_LOGIC.")
    }
    logger.info(s"Data written to target ${jobConfig.target_table_or_path} for job ${jobConfig.job_name}")

    if (jobConfig.audit_level.toUpperCase != "NONE") {
        try {
            val targetFormat = jobConfig.target_format.getOrElse("parquet")
            val finalTargetDF = jobConfig.target_type.toUpperCase match {
                case "HDFS" | "FILE" => spark.read.format(targetFormat).load(jobConfig.target_table_or_path)
                case "HIVE_TABLE" => spark.table(jobConfig.target_table_or_path)
                case _ => throw new RuntimeException("Cannot re-read target for audit count.")
            }
            recordsWrittenCount = Some(finalTargetDF.count())
        } catch { case e: Exception => logger.warn(s"Could not count written records for ${jobConfig.job_name}: ${e.getMessage}", e) }\n    }\n\n    if (jobConfig.reconciliation_enabled.equalsIgnoreCase("Y") && jobConfig.reconciliation_config.exists(_.trim.nonEmpty)) {\n      logger.info(s"Starting reconciliation for ${jobConfig.job_name}...")\n      try {\n        val sourceForReconDF = transformedDF\n        val targetSnapshotDF = jobConfig.target_type.toUpperCase match {\n            case "HDFS" | "FILE" => spark.read.format(jobConfig.target_format.getOrElse("parquet")).load(jobConfig.target_table_or_path)\n            case "HIVE_TABLE" => spark.table(jobConfig.target_table_or_path)\n            case other => throw new RuntimeException("Cannot get target snapshot for recon for unsupported target type " + other)\n        }\n        val report = ReconciliationService.executeChecks(jobConfig.job_name, sourceForReconDF, targetSnapshotDF, jobConfig.reconciliation_config.get, spark)\n        logger.info(s"Recon Report for ${jobConfig.job_name}: Status - ${report.overallStatus}. Details: ${report.checkResults.map(r => s"${r.checkType}:${r.status}").mkString("; ")}")\n        if (report.overallStatus != "PASS") logger.warn(s"Recon for ${jobConfig.job_name} status: ${report.overallStatus}.")\n      } catch { case e: Exception => logger.error(s"Error during recon for ${jobConfig.job_name}: ${e.getMessage}", e) }\n    }\n\n    // --- Data Quality Checks --- \n    if (jobConfig.dq_checks_enabled.equalsIgnoreCase("Y") && jobConfig.dq_rules_config.exists(_.trim.nonEmpty)) {\n      logger.info(s"Starting Data Quality checks for job ${jobConfig.job_name} on target data...")\n      try {\n        val targetSnapshotDF = jobConfig.target_type.toUpperCase match {\n            case "HDFS" | "FILE" => spark.read.format(jobConfig.target_format.getOrElse("parquet")).load(jobConfig.target_table_or_path)\n            case "HIVE_TABLE" => spark.table(jobConfig.target_table_or_path)\n            case other => throw new RuntimeException("Cannot get target snapshot for DQ for unsupported target type " + other)\n        }\n        val dqReport = DataQualityService.applyChecks(jobConfig.job_name, targetSnapshotDF, jobConfig.dq_rules_config.get, spark)\n        dataQualityReportOpt = Some(dqReport)\n        logger.info(s"Data Quality Report for job ${jobConfig.job_name}: Status - ${dqReport.overallStatus}")\n        dqReport.rule_results.foreach(rr => logger.info(s"  DQ Rule: ${rr.rule_name}, Status: ${rr.status}, Failing: ${rr.failing_records_count.getOrElse("N/A")}, Msg: ${rr.message}"))\n        if (dqReport.overallStatus == "FAIL") {\n          logger.error(s"Critical DQ checks failed for job ${jobConfig.job_name}. Overall Status: FAIL. Failing job.")\n          throw new RuntimeException(s"Critical DQ checks failed for job ${jobConfig.job_name}. Status: FAIL.")\n        } else if (dqReport.overallStatus == "WARN") {\n           logger.warn(s"DQ checks passed with warnings for job ${jobConfig.job_name}. Status: WARN.")\n        }\n      } catch {\n        case e: RuntimeException if e.getMessage.startsWith("Critical DQ checks failed") => throw e // Re-throw to fail job\n        case e: Exception => \n          logger.error(s"Error during DQ process for job ${jobConfig.job_name}: ${e.getMessage}", e)\n          throw new RuntimeException(s"Error in DQ subsystem for job ${jobConfig.job_name}", e) // Fail job on DQ system error\n      }\n    }\n\n    logger.info(s"WITH_LOGIC ingestion for job_name: ${jobConfig.job_name} completed successfully.")\n    (recordsReadCount, recordsWrittenCount, dataQualityReportOpt) // Updated return tuple\n  }\n}
