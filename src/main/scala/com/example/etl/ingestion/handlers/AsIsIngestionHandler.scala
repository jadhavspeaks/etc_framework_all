package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.util.SchemaMapper
import com.example.etl.reconciliation.ReconciliationService // Import ReconciliationService
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON

object AsIsIngestionHandler {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long]) = {
    logger.info(s"Executing AS_IS ingestion for job_id: ${jobConfig.job_id}")
    var recordsReadCount: Option[Long] = None
    var recordsWrittenCount: Option[Long] = None

    if (jobConfig.source_connection_details.isEmpty) {
      throw new IllegalArgumentException(s"Source connection details (path/table) are required for job ${jobConfig.job_id}")
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
            case _ => logger.warn(s"Could not parse or apply source_format_options: $optionsStr")
          }
        }
        jobConfig.source_schema.foreach{ schemaDDL => reader.schema(schemaDDL) }
        sourceDF = reader.load(sourcePath)
      case other =>
        throw new UnsupportedOperationException(s"Source type '$other' is not yet supported.")
    }

    if (sourceDF == null) {
        throw new RuntimeException("Source DataFrame could not be loaded.")
    }

    if (jobConfig.audit_level.toUpperCase != "NONE") {
        try {
            recordsReadCount = Some(sourceDF.count())
            logger.info(s"Records read from source: ${recordsReadCount.get}")
        } catch {
            case e: Exception => logger.warn(s"Could not count source records: ${e.getMessage}", e)
        }
    }

    logger.info("Source data read successfully.")
    if (logger.isDebugEnabled) sourceDF.printSchema()

    var transformedDF = sourceDF
    jobConfig.schema_mapping_logic match {
      case Some(mappingJsonString) if mappingJsonString.trim.nonEmpty =>
        logger.info(s"Applying schema mapping logic: $mappingJsonString")
        val mappingInstructions = SchemaMapper.parseMappingLogic(mappingJsonString)
        if (mappingInstructions.nonEmpty) {
          val newSourceColBehavior = jobConfig.job_properties.flatMap(propsJson =>
            JSON.parseFull(propsJson) match {
              case Some(map: Map[String, String]) => map.get("schema_drift_new_source_columns_behavior")
              case _ => None
            }
          ).getOrElse("ignore")
          logger.info(s"Schema drift behavior for new source columns: '$newSourceColBehavior'")
          transformedDF = SchemaMapper.applyMapping(sourceDF, mappingInstructions, spark, newSourceColBehavior)
        } else {
          logger.warn("Parsed schema mapping logic resulted in no instructions. No mapping applied.")
        }
      case _ =>
        logger.info("No schema_mapping_logic provided or it is empty. DataFrame will not be transformed by SchemaMapper.")
    }

    if (logger.isDebugEnabled) {
        logger.debug("Schema after SchemaMapper transformation:")
        transformedDF.printSchema()
        logger.debug("Sample data after SchemaMapper transformation:")
        transformedDF.show(5, truncate=false)
    }

    logger.info(s"Writing target: ${jobConfig.target_type}, format: ${jobConfig.target_format.getOrElse("N/A")}, path/table: ${jobConfig.target_table_or_path}")
    val writer = transformedDF.write
    jobConfig.target_format.foreach(format => writer.format(format.toLowerCase))
    jobConfig.target_format_options.foreach { optionsStr =>
      JSON.parseFull(optionsStr) match {
        case Some(map: Map[String, String]) => writer.options(map)
        case _ => logger.warn(s"Could not parse or apply target_format_options: $optionsStr")
      }
    }
    jobConfig.partitioning_columns.foreach { pc =>
      val columns = pc.split(',').map(_.trim).filter(_.nonEmpty)
      if (columns.nonEmpty) writer.partitionBy(columns: _*)\n    }\n    val saveMode = jobConfig.target_write_mode.toLowerCase match {\n      case "overwrite" => SaveMode.Overwrite\n      case "append" => SaveMode.Append\n      case "ignore" => SaveMode.Ignore\n      case "errorifexists" | "error" => SaveMode.ErrorIfExists\n      case _ => \n        logger.warn(s"Unsupported save mode '${jobConfig.target_write_mode}'. Defaulting to 'Overwrite'.")\n        SaveMode.Overwrite\n    }\n    writer.mode(saveMode)\n    \n    jobConfig.target_type.toUpperCase match {\n      case "HDFS" | "FILE" =>\n        writer.save(jobConfig.target_table_or_path)\n        if (jobConfig.audit_level.toUpperCase != "NONE") {\n            try {\n                val targetFormatToRead = jobConfig.target_format.getOrElse(jobConfig.source_format.getOrElse("parquet"))\n                recordsWrittenCount = Some(spark.read.format(targetFormatToRead).load(jobConfig.target_table_or_path).count())\n                logger.info(s"Records written to target (re-read for count): ${recordsWrittenCount.get}")\n            } catch {\n                case e: Exception => logger.warn(s"Could not count written records by re-reading: ${e.getMessage}", e)\n            }\n        }\n      case other =>\n        throw new UnsupportedOperationException(s"Target type '$other' is not yet supported.")\n    }\n\n    // --- Perform Reconciliation --- \n    if (jobConfig.reconciliation_enabled.equalsIgnoreCase("Y") && jobConfig.reconciliation_config.exists(_.trim.nonEmpty)) {\n      logger.info("Starting reconciliation process...")\n      try {\n        val sourceForReconDF = transformedDF // DataFrame that was written\n        val targetFormatForRead = jobConfig.target_format.getOrElse(if(jobConfig.source_type.toUpperCase == "FILE") jobConfig.source_format.getOrElse("parquet") else "parquet")\n        logger.info(s"Re-reading target data from ${jobConfig.target_table_or_path} using format $targetFormatForRead for reconciliation.")\n        val targetSnapshotDF = spark.read.format(targetFormatForRead).load(jobConfig.target_table_or_path)\n        \n        val report = ReconciliationService.executeChecks(jobConfig.job_id, sourceForReconDF, targetSnapshotDF, jobConfig.reconciliation_config.get, spark)\n        logger.info(s"Reconciliation Report for job ${jobConfig.job_id}: Status - ${report.overallStatus}")\n        report.checkResults.foreach(cr => logger.info(s"  Check: ${cr.checkType}, Status: ${cr.status}, SrcVal: ${cr.sourceValue}, TgtVal: ${cr.targetValue}, Diff: ${cr.difference.getOrElse("N/A")}, Msg: ${cr.message}"))\n        if (report.overallStatus != "PASS") {\n          logger.warn(s"Reconciliation for job ${jobConfig.job_id} resulted in status: ${report.overallStatus}. Review details above.")\n          // Optionally, could throw an exception here if jobConfig.fail_job_on_reconciliation_failure is true\n        }\n      } catch {\n        case e: Exception => \n          logger.error(s"Error during reconciliation process for job ${jobConfig.job_id}: ${e.getMessage}", e)\n          // Do not fail the job, but log error clearly\n      }\n    }\n\n    logger.info(s"AS_IS ingestion for job_id: ${jobConfig.job_id} completed successfully.")\n    (recordsReadCount, recordsWrittenCount)\n  }\n}
