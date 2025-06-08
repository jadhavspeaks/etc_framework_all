package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.util.SchemaMapper
import com.example.etl.reconciliation.ReconciliationService
import com.example.etl.dq.{DataQualityService, DataQualityReport} // Import DQ types
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON

object AsIsIngestionHandler {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long], Option[DataQualityReport]) = { // Updated signature
    logger.info(s"Executing AS_IS ingestion for job_name: ${jobConfig.job_name}")
    var recordsReadCount: Option[Long] = None
    var recordsWrittenCount: Option[Long] = None
    var dataQualityReportOpt: Option[DataQualityReport] = None // For DQ results

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
      case "HIVE_TABLE" => // Assuming HIVE_TABLE source might be added to AS_IS later
        logger.info(s"Reading HIVE_TABLE source: ${jobConfig.source_connection_details.get} for job ${jobConfig.job_name}")
        try { sourceDF = spark.table(jobConfig.source_connection_details.get) } catch { case e: Exception => throw new RuntimeException(s"Failed to read HIVE_TABLE ${jobConfig.source_connection_details.get}: ${e.getMessage}", e)}
      case other =>
        throw new UnsupportedOperationException(s"Source type '$other' is not yet supported for AS_IS handler.")
    }

    if (sourceDF == null) throw new RuntimeException("Source DataFrame could not be loaded.")
    if (jobConfig.audit_level.toUpperCase != "NONE") { try { recordsReadCount = Some(sourceDF.count()) } catch { case e: Exception => logger.warn(s"Could not count source records for ${jobConfig.job_name}: ${e.getMessage}", e) } }
    logger.info(s"Source data read for ${jobConfig.job_name}. Records: ${recordsReadCount.getOrElse("N/A")}")
    if (logger.isDebugEnabled) sourceDF.printSchema()

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

    if (logger.isDebugEnabled) { logger.debug("Schema after SchemaMapper:"); transformedDF.printSchema(); transformedDF.show(5, truncate=false) }

    val writer = transformedDF.write
    jobConfig.target_format.foreach(f => writer.format(f.toLowerCase))
    jobConfig.target_format_options.foreach { o => JSON.parseFull(o) match {case Some(m:Map[String,String])=>writer.options(m) case _ => }}
    jobConfig.partitioning_columns.foreach { pc => val c=pc.split(',').map(_.trim).filter(_.nonEmpty); if(c.nonEmpty) writer.partitionBy(c:_*) }
    val sm = jobConfig.target_write_mode.toLowerCase match { case "overwrite"=>SaveMode.Overwrite case "append"=>SaveMode.Append case "ignore"=>SaveMode.Ignore case "errorifexists"|"error"=>SaveMode.ErrorIfExists case _=>SaveMode.Overwrite }
    writer.mode(sm)

    jobConfig.target_type.toUpperCase match {
      case "HDFS" | "FILE" => writer.save(jobConfig.target_table_or_path)
      case "HIVE_TABLE" => writer.saveAsTable(jobConfig.target_table_or_path)
      case other => throw new UnsupportedOperationException(s"Target type '$other' not supported by AS_IS handler.")
    }
    logger.info(s"Data written to target ${jobConfig.target_table_or_path} for job ${jobConfig.job_name}")

    if (jobConfig.audit_level.toUpperCase != "NONE") {
        try {
            val targetFormat = jobConfig.target_format.getOrElse("parquet") // Default for re-read
            val finalTargetDF = jobConfig.target_type.toUpperCase match {
                case "HDFS" | "FILE" => spark.read.format(targetFormat).load(jobConfig.target_table_or_path)
                case "HIVE_TABLE" => spark.table(jobConfig.target_table_or_path)
                case _ => throw new RuntimeException("Cannot re-read target for audit count due to unsupported target type.")
            }
            recordsWrittenCount = Some(finalTargetDF.count())
            logger.info(s"Records written to target for ${jobConfig.job_name} (audited by re-read): ${recordsWrittenCount.get}")
        } catch { case e: Exception => logger.warn(s"Could not count written records by re-reading for ${jobConfig.job_name}: ${e.getMessage}", e) }\n    }\n\n    if (jobConfig.reconciliation_enabled.equalsIgnoreCase("Y") && jobConfig.reconciliation_config.exists(_.trim.nonEmpty)) {\n      logger.info(s"Starting reconciliation for job ${jobConfig.job_name}...")\n      try {\n        val sourceForReconDF = transformedDF\n        val targetFormatForRead = jobConfig.target_format.getOrElse("parquet")\n        val targetSnapshotDF = jobConfig.target_type.toUpperCase match {\n            case "HDFS" | "FILE" => spark.read.format(targetFormatForRead).load(jobConfig.target_table_or_path)\n            case "HIVE_TABLE" => spark.table(jobConfig.target_table_or_path)\n            case other => throw new RuntimeException("Cannot get target snapshot for recon for unsupported target type " + other)\n        }\n        val report = ReconciliationService.executeChecks(jobConfig.job_name, sourceForReconDF, targetSnapshotDF, jobConfig.reconciliation_config.get, spark)\n        logger.info(s"Recon Report for ${jobConfig.job_name}: Status - ${report.overallStatus}. Details: ${report.checkResults.map(r => s"${r.checkType}:${r.status}").mkString("; ")}")\n        if (report.overallStatus != "PASS") logger.warn(s"Recon for ${jobConfig.job_name} status: ${report.overallStatus}.")\n      } catch { case e: Exception => logger.error(s"Error during recon for ${jobConfig.job_name}: ${e.getMessage}", e) }\n    }\n\n    // --- Data Quality Checks --- \n    if (jobConfig.dq_checks_enabled.equalsIgnoreCase("Y") && jobConfig.dq_rules_config.exists(_.trim.nonEmpty)) {\n      logger.info(s"Starting Data Quality checks for job ${jobConfig.job_name} on target data...")\n      try {\n        val targetFormatForRead = jobConfig.target_format.getOrElse("parquet")\n        val targetSnapshotDF = jobConfig.target_type.toUpperCase match {\n            case "HDFS" | "FILE" => spark.read.format(targetFormatForRead).load(jobConfig.target_table_or_path)\n            case "HIVE_TABLE" => spark.table(jobConfig.target_table_or_path)\n            case other => throw new RuntimeException("Cannot get target snapshot for DQ for unsupported target type " + other)\n        }\n        val dqReport = DataQualityService.applyChecks(jobConfig.job_name, targetSnapshotDF, jobConfig.dq_rules_config.get, spark)\n        dataQualityReportOpt = Some(dqReport)\n        logger.info(s"Data Quality Report for job ${jobConfig.job_name}: Status - ${dqReport.overallStatus}")\n        dqReport.rule_results.foreach(rr => logger.info(s"  DQ Rule: ${rr.rule_name} (${rr.rule_type}), Status: ${rr.status}, Failing Count: ${rr.failing_records_count.getOrElse("N/A")}, Msg: ${rr.message}"))\n        if (dqReport.overallStatus == "FAIL") { // FAIL means a FAIL_JOB level rule failed\n          logger.error(s"Critical Data Quality checks failed for job ${jobConfig.job_name}. Overall DQ Status: FAIL. Failing job.")\n          throw new RuntimeException(s"Critical Data Quality checks failed. Overall DQ Status: FAIL for job ${jobConfig.job_name}")\n        } else if (dqReport.overallStatus == "WARN") {\n           logger.warn(s"Data Quality checks passed with warnings for job ${jobConfig.job_name}. Overall DQ Status: WARN.")\n        }\n      } catch {\n        case e: Exception => \n          logger.error(s"Error during Data Quality check process for job ${jobConfig.job_name}: ${e.getMessage}", e)\n          // Optionally, re-throw if DQ errors should always fail the job, or make this configurable\n          throw new RuntimeException(s"Error in Data Quality subsystem for job ${jobConfig.job_name}", e)\n      }\n    }\n\n    logger.info(s"AS_IS ingestion for job_name: ${jobConfig.job_name} completed successfully.")\n    (recordsReadCount, recordsWrittenCount, dataQualityReportOpt) // Updated return tuple\n  }\n}
