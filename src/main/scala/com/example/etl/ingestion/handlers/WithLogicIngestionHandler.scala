package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.sql.{SqlParserService, LogicalPlanValidator}
import com.example.etl.reconciliation.ReconciliationService // Import ReconciliationService
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode, Dataset}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON

object WithLogicIngestionHandler {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val DefaultSourceViewName = "source_view"

  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long]) = {
    logger.info(s"Executing WITH_LOGIC ingestion for job_id: ${jobConfig.job_id}")
    var recordsReadCount: Option[Long] = None
    var recordsWrittenCount: Option[Long] = None

    if (jobConfig.source_connection_details.isEmpty) {
      throw new IllegalArgumentException(s"Source connection details are required for job ${jobConfig.job_id}")
    }
    val sourcePath = jobConfig.source_connection_details.get
    var sourceDF: DataFrame = null

    jobConfig.source_type.toUpperCase match {
      case "FILE" =>
        val format = jobConfig.source_format.getOrElse(throw new IllegalArgumentException("source_format is required for FILE source_type")).toLowerCase
        val reader = spark.read.format(format)
        jobConfig.source_format_options.foreach { optionsStr =>
          JSON.parseFull(optionsStr) match {
            case Some(map: Map[String, String]) => reader.options(map)
            case _ => logger.warn(s"Could not parse source_format_options: $optionsStr")
          }
        }
        jobConfig.source_schema.foreach{ schemaDDL => reader.schema(schemaDDL) }
        sourceDF = reader.load(sourcePath)
      case other => throw new UnsupportedOperationException(s"Source type '$other' not supported yet.")
    }

    if (sourceDF == null) {
      throw new RuntimeException("Source DataFrame could not be loaded.")
    }

    if (jobConfig.audit_level.toUpperCase != "NONE") {
      try { recordsReadCount = Some(sourceDF.count()) } catch { case e: Exception => logger.warn(s"Could not count source records: ${e.getMessage}", e) }\n    }\n    logger.info(s"Source data read successfully. Records read: ${recordsReadCount.getOrElse("N/A")}")
    if (logger.isDebugEnabled) sourceDF.printSchema()

    val sourceViewName = jobConfig.job_name + "_source_view_wl" // Unique view name
    sourceDF.createOrReplaceTempView(sourceViewName)
    logger.info(s"Registered source DataFrame as temporary view: '$sourceViewName'")

    val sqlLogic = jobConfig.sql_logic.getOrElse(throw new IllegalArgumentException(s"sql_logic is required for WITH_LOGIC mode in job ${jobConfig.job_id}"))
    if (sqlLogic.trim.isEmpty) {
      throw new IllegalArgumentException(s"sql_logic cannot be empty for WITH_LOGIC mode in job ${jobConfig.job_id}")
    }
    logger.info(s"SQL Logic to be applied (first 200 chars): ${sqlLogic.take(200)}...")

    val logicalPlanEither = SqlParserService.parse(sqlLogic, spark)
    val logicalPlan = logicalPlanEither match {
      case Right(plan) => logger.info("SQL parsed successfully to LogicalPlan."); plan
      case Left(errorMsg) => throw new RuntimeException(s"Failed to parse SQL logic: $errorMsg")
    }

    LogicalPlanValidator.validate(logicalPlan) match {
      case Right(_) => logger.info("LogicalPlan validation successful.")
      case Left(errors) => throw new RuntimeException(s"LogicalPlan validation failed: ${errors.mkString("; ")}")
    }

    logger.info("Executing LogicalPlan to get transformed DataFrame...")
    val transformedDF: DataFrame = try {
      Dataset.ofRows(spark, logicalPlan)
    } catch {
      case e: Exception => throw new RuntimeException(s"Error during DataFrame generation from LogicalPlan: ${e.getMessage}", e)
    }
    logger.info("Transformed DataFrame generated successfully from LogicalPlan.")
    if (logger.isDebugEnabled) transformedDF.printSchema()

    // It's important that recordsWrittenCount here reflects the count of transformedDF, if that's the primary output.
    // Or, it should be the count of what's actually written to the final target.
    // For now, we count what's written by re-reading (consistent with AsIsHandler).

    val writer = transformedDF.write
    jobConfig.target_format.foreach(format => writer.format(format.toLowerCase))
    jobConfig.target_format_options.foreach { optionsStr =>
      JSON.parseFull(optionsStr) match {
        case Some(map: Map[String, String]) => writer.options(map)
        case _ => logger.warn(s"Could not parse target_format_options: $optionsStr")
      }
    }
    jobConfig.partitioning_columns.foreach { pc =>
      val columns = pc.split(',').map(_.trim).filter(_.nonEmpty)
      if (columns.nonEmpty) writer.partitionBy(columns: _*)\n    }\n    val saveMode = jobConfig.target_write_mode.toLowerCase match {\n      case "overwrite" => SaveMode.Overwrite\n      case "append" => SaveMode.Append\n      case "ignore" => SaveMode.Ignore\n      case "errorifexists" | "error" => SaveMode.ErrorIfExists\n      case _ => SaveMode.Overwrite\n    }\n    writer.mode(saveMode)\n    \n    jobConfig.target_type.toUpperCase match {\n      case "HDFS" | "FILE" =>\n        writer.save(jobConfig.target_table_or_path)\n        if (jobConfig.audit_level.toUpperCase != "NONE") {\n            try {\n                val targetFormatToRead = jobConfig.target_format.getOrElse("parquet") // Simpler fallback for target read\n                recordsWrittenCount = Some(spark.read.format(targetFormatToRead).load(jobConfig.target_table_or_path).count())\n            } catch { case e: Exception => logger.warn(s"Could not count written records for audit: ${e.getMessage}", e) }\n        }\n      case other => throw new UnsupportedOperationException(s"Target type '$other' not supported yet.")\n    }\n\n    // --- Perform Reconciliation --- \n    if (jobConfig.reconciliation_enabled.equalsIgnoreCase("Y") && jobConfig.reconciliation_config.exists(_.trim.nonEmpty)) {\n      logger.info("Starting reconciliation process for WITH_LOGIC handler...")\n      try {\n        val sourceForReconDF = transformedDF // DataFrame that was written\n        val targetFormatForRead = jobConfig.target_format.getOrElse("parquet")\n        logger.info(s"Re-reading target data from ${jobConfig.target_table_or_path} using format $targetFormatForRead for reconciliation.")\n        val targetSnapshotDF = spark.read.format(targetFormatForRead).load(jobConfig.target_table_or_path)\n        \n        val report = ReconciliationService.executeChecks(jobConfig.job_id, sourceForReconDF, targetSnapshotDF, jobConfig.reconciliation_config.get, spark)\n        logger.info(s"Reconciliation Report for job ${jobConfig.job_id} (WITH_LOGIC): Status - ${report.overallStatus}")\n        report.checkResults.foreach(cr => logger.info(s"  Check: ${cr.checkType}, Status: ${cr.status}, SrcVal: ${cr.sourceValue}, TgtVal: ${cr.targetValue}, Diff: ${cr.difference.getOrElse("N/A")}, Msg: ${cr.message}"))\n        if (report.overallStatus != "PASS") {\n          logger.warn(s"Reconciliation for job ${jobConfig.job_id} (WITH_LOGIC) resulted in status: ${report.overallStatus}.")\n        }\n      } catch {\n        case e: Exception => \n          logger.error(s"Error during reconciliation process for job ${jobConfig.job_id} (WITH_LOGIC): ${e.getMessage}", e)\n      }\n    }\n\n    logger.info(s"WITH_LOGIC ingestion for job_id: ${jobConfig.job_id} completed successfully. Records written: ${recordsWrittenCount.getOrElse("N/A")}")\n    // The first element of the tuple should ideally be the count of records *input* to the SQL logic if possible,\n    // or recordsReadCount if the SQL operates on the entire source. For now, using recordsReadCount (raw source).\n    // A more accurate 'processed' count would be transformedDF.count() if it's cheap, or just pass it as recordsWrittenCount if it's the final dataset.\n    (recordsReadCount, recordsWrittenCount) \n  }\n}
