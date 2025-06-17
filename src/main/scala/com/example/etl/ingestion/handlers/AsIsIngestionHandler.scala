package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.util.{SchemaMapper, LoggingUtil} // Import LoggingUtil
import com.example.etl.reconciliation.ReconciliationService
import com.example.etl.dq.{DataQualityService, DataQualityReport}
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON

object AsIsIngestionHandler {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long], Option[DataQualityReport]) = {
    logger.info(s"Executing AS_IS ingestion for job_name: ${jobConfig.job_name}")
    var recordsReadCount: Option[Long] = None
    var recordsWrittenCount: Option[Long] = None
    var dataQualityReportOpt: Option[DataQualityReport] = None

    if (jobConfig.source_connection_details.isEmpty) {
      throw new IllegalArgumentException(s"Source connection details (path/table) are required for job ${jobConfig.job_name}")
    }
    val sourcePath = jobConfig.source_connection_details.get
    var sourceDF: DataFrame = null

    jobConfig.source_type.toUpperCase match {
      case "FILE" =>
        val format = jobConfig.source_format.getOrElse(throw new IllegalArgumentException("source_format is required for FILE source_type")).toLowerCase
        val reader = spark.read.format(format)
        jobConfig.source_format_options.foreach { o => JSON.parseFull(o) match {case Some(m:Map[String,String])=>reader.options(m) case _=>logger.warn(s"Could not parse source_opts: $o")} }
        jobConfig.source_schema.foreach{ s => reader.schema(s) }
        sourceDF = reader.load(sourcePath)
      case "HIVE_TABLE" =>
        logger.info(s"Reading HIVE_TABLE source: ${jobConfig.source_connection_details.get} for job ${jobConfig.job_name}")
        try { sourceDF = spark.table(jobConfig.source_connection_details.get) } catch { case e: Exception => throw new RuntimeException(s"Failed to read HIVE_TABLE ${jobConfig.source_connection_details.get}: ${e.getMessage}", e)}
      case other =>
        throw new UnsupportedOperationException(s"Source type '$other' is not yet supported for AS_IS handler.")
    }

    if (sourceDF == null) throw new RuntimeException("Source DataFrame could not be loaded.")
    if (jobConfig.audit_level.toUpperCase != "NONE") { try { recordsReadCount = Some(sourceDF.count()) } catch { case e: Exception => logger.warn(s"Could not count source records for ${jobConfig.job_name}: ${e.getMessage}", e) } }
    logger.info(s"Source data read for ${jobConfig.job_name}. Records: ${recordsReadCount.getOrElse("N/A")}")
    if (logger.isDebugEnabled) {
      logger.debug(s"Schema for sourceDF (job ${jobConfig.job_name}):")
      sourceDF.printSchema()
      logger.debug(s"Sample data from sourceDF (job ${jobConfig.job_name}), potentially masked:")
      val dfToShow = jobConfig.log_masking_columns.filter(_.trim.nonEmpty) match {
        case Some(cols) => LoggingUtil.maskDataFrame(sourceDF, cols.split(',').map(_.trim))
        case None => sourceDF
      }
      dfToShow.show(5, truncate=false)
    }

    var transformedDF = sourceDF
    jobConfig.schema_mapping_logic match {
      case Some(mj) if mj.trim.nonEmpty =>
        val mi = SchemaMapper.parseMappingLogic(mj)
        if (mi.nonEmpty) {
          val driftBehavior = jobConfig.job_properties.flatMap(pJson => JSON.parseFull(pJson).collect{case m:Map[String,String]=>m.get("schema_drift_new_source_columns_behavior")}.flatten).getOrElse("ignore")
          transformedDF = SchemaMapper.applyMapping(sourceDF, mi, spark, driftBehavior)
        } else logger.warn("Schema_mapping_logic provided but no instructions parsed.")
      case _ => logger.info("No schema_mapping_logic provided.")
    }

    if (logger.isDebugEnabled && transformedDF ne sourceDF) { // Only show if it's different from sourceDF
        logger.debug(s"Schema after SchemaMapper (job ${jobConfig.job_name}):")
        transformedDF.printSchema()
        logger.debug(s"Sample data after SchemaMapper (job ${jobConfig.job_name}), potentially masked:")
        val dfToShow = jobConfig.log_masking_columns.filter(_.trim.nonEmpty) match {
          case Some(cols) => LoggingUtil.maskDataFrame(transformedDF, cols.split(',').map(_.trim))
          case None => transformedDF
        }
        dfToShow.show(5, truncate=false)
    }

    val writer = transformedDF.write
    jobConfig.target_format.foreach(f => writer.format(f.toLowerCase))
    jobConfig.target_format_options.foreach { o => JSON.parseFull(o) match {case Some(m:Map[String,String])=>writer.options(m) case _ => }}
    jobConfig.partitioning_columns.foreach { pc => val c=pc.split(',').map(_.trim).filter(_.nonEmpty); if(c.nonEmpty) writer.partitionBy(c:_*) }
    val sm = jobConfig.target_write_mode.toLowerCase match { case "overwrite"=>SaveMode.Overwrite case "append"=>SaveMode.Append case "ignore"=>SaveMode.Ignore case "errorifexists"|"error"=>SaveMode.ErrorIfExists case _=>SaveMode.Overwrite }
    writer.mode(sm)

    val targetIdentifier = jobConfig.target_table_or_path
    jobConfig.target_type.toUpperCase match {
      case "HDFS" | "FILE" => writer.save(targetIdentifier)
      case "HIVE_TABLE" => writer.saveAsTable(targetIdentifier)
      case other => throw new UnsupportedOperationException(s"Target type '$other' not supported by AS_IS handler for job ${jobConfig.job_name}.")
    }
    logger.info(s"Data written to target $targetIdentifier for job ${jobConfig.job_name}")

    var finalTargetDFForAudit: DataFrame = null // For audit count and reconciliation
    if (jobConfig.audit_level.toUpperCase != "NONE" || jobConfig.reconciliation_enabled.equalsIgnoreCase("Y") || jobConfig.dq_checks_enabled.equalsIgnoreCase("Y")) {
        try {
            val targetFormat = jobConfig.target_format.getOrElse("parquet")
            finalTargetDFForAudit = jobConfig.target_type.toUpperCase match {
                case "HDFS" | "FILE" => spark.read.format(targetFormat).load(targetIdentifier)
                case "HIVE_TABLE" => spark.table(targetIdentifier)
                case _ => throw new RuntimeException(s"Cannot re-read target for post-processing for unsupported target type in job ${jobConfig.job_name}.")
            }
            if (jobConfig.audit_level.toUpperCase != "NONE") {
                recordsWrittenCount = Some(finalTargetDFForAudit.count())
                logger.info(s"Records written to target for ${jobConfig.job_name} (audited by re-read): ${recordsWrittenCount.get}")
            }
        } catch { case e: Exception => logger.warn(s"Could not re-read target for audit/recon/dq for ${jobConfig.job_name}: ${e.getMessage}", e) }\n    }\n\n    if (jobConfig.reconciliation_enabled.equalsIgnoreCase("Y") && jobConfig.reconciliation_config.exists(_.trim.nonEmpty) && finalTargetDFForAudit != null) {\n      logger.info(s"Starting reconciliation for job ${jobConfig.job_name}...")\n      try {\n        val report = ReconciliationService.executeChecks(jobConfig.job_name, transformedDF, finalTargetDFForAudit, jobConfig.reconciliation_config.get, spark)\n        logger.info(s"Recon Report for ${jobConfig.job_name}: Status - ${report.overallStatus}. Details: ${report.checkResults.map(r => s"${r.checkType}:${r.status}").mkString("; ")}")\n        if (report.overallStatus != "PASS") logger.warn(s"Recon for ${jobConfig.job_name} status: ${report.overallStatus}.")\n      } catch { case e: Exception => logger.error(s"Error during recon for ${jobConfig.job_name}: ${e.getMessage}", e) }\n    }\n\n    if (jobConfig.dq_checks_enabled.equalsIgnoreCase("Y") && jobConfig.dq_rules_config.exists(_.trim.nonEmpty) && finalTargetDFForAudit != null) {\n      logger.info(s"Starting Data Quality checks for job ${jobConfig.job_name} on target data...")\n      try {\n        val dqReport = DataQualityService.applyChecks(jobConfig.job_name, finalTargetDFForAudit, jobConfig.dq_rules_config.get, spark)\n        dataQualityReportOpt = Some(dqReport)\n        logger.info(s"Data Quality Report for job ${jobConfig.job_name}: Status - ${dqReport.overallStatus}")\n        dqReport.rule_results.foreach(rr => logger.info(s"  DQ Rule: ${rr.rule_name} (${rr.rule_type}), Status: ${rr.status}, Failing Count: ${rr.failing_records_count.getOrElse("N/A")}, Msg: ${rr.message}"))\n        if (dqReport.overallStatus == "FAIL") {\n          logger.error(s"Critical DQ checks failed for job ${jobConfig.job_name}. Overall Status: FAIL. Failing job.")\n          throw new RuntimeException(s"Critical Data Quality checks failed. Overall DQ Status: FAIL for job ${jobConfig.job_name}")\n        }\n      } catch {\n        case e: RuntimeException if e.getMessage.startsWith("Critical DQ checks failed") => throw e \n        case e: Exception => \n          logger.error(s"Error during DQ process for job ${jobConfig.job_name}: ${e.getMessage}", e)\n          throw new RuntimeException(s"Error in DQ subsystem for job ${jobConfig.job_name}", e) \n      }\n    }\n\n    logger.info(s"AS_IS ingestion for job_name: ${jobConfig.job_name} completed successfully.")\n    (recordsReadCount, recordsWrittenCount, dataQualityReportOpt) \n  }\n}
