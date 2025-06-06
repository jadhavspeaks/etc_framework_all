package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.sql.{SqlParserService, LogicalPlanValidator}
import com.example.etl.logic.Scd2LogicUtil
import com.example.etl.reconciliation.ReconciliationService // Import ReconciliationService
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, StructType, StructField}// Added StructType, StructField for empty DF schema
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.time.Instant
import scala.util.parsing.json.JSON

object Scd2LogicHandler {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long]) = {
    logger.info(s"Executing SCD Type 2 ingestion for job_id: ${jobConfig.job_id}")

    val scdProcessingTimestamp = Timestamp.from(Instant.now())
    val scdHighDateTimestamp = Timestamp.valueOf("9999-12-31 23:59:59")

    var initialSourceRecordsCount: Option[Long] = None
    var transformedSourceRecordsCount: Option[Long] = None
    var targetCurrentRecordsCount: Option[Long] = None
    var recordsWrittenCount: Option[Long] = None

    val scdCols = Scd2LogicUtil.ScdColumnNames(
      skIdCol = jobConfig.scd2_surrogate_key_column.getOrElse("sk_id"),
      validFromCol = jobConfig.scd2_valid_from_column.getOrElse("valid_from_ts"),
      validToCol = jobConfig.scd2_valid_to_column.getOrElse("valid_to_ts"),
      versionCol = jobConfig.scd2_version_column.getOrElse("version"),
      isCurrentCol = jobConfig.scd2_current_flag_column.getOrElse("is_current"),
      changeTypeCol = "change_type"
    )

    if (jobConfig.source_connection_details.isEmpty) {
      throw new IllegalArgumentException(s"Source connection details are required for job ${jobConfig.job_id}")
    }
    val rawSourceDF: DataFrame = jobConfig.source_type.toUpperCase match {
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
        reader.load(jobConfig.source_connection_details.get)
      case other => throw new UnsupportedOperationException(s"Source type '$other' not supported yet for SCD2 handler.")
    }
    if (jobConfig.audit_level.toUpperCase != "NONE") {
        try { initialSourceRecordsCount = Some(rawSourceDF.count()) } catch { case e: Exception => logger.warn(s"Could not count raw source records: ${e.getMessage}", e) }\n    }\n    logger.info(s"Raw source data read. Records: ${initialSourceRecordsCount.getOrElse("N/A")}")

    val transformedSourceDF: DataFrame = jobConfig.sql_logic match {
      case Some(sql) if sql.trim.nonEmpty =>
        logger.info(s"Applying sql_logic for SCD source transformation. SQL: ${sql.take(200)}...")
        val rawSourceViewName = jobConfig.job_name + "_raw_source_view_for_scd_transform"
        rawSourceDF.createOrReplaceTempView(rawSourceViewName)
        logger.info(s"Registered raw source DataFrame as temp view: '$rawSourceViewName'")

        val logicalPlanEither = SqlParserService.parse(sql, spark)
        val logicalPlan = logicalPlanEither match {
          case Right(plan) => logger.info("SCD transform SQL parsed successfully."); plan
          case Left(errorMsg) => throw new RuntimeException(s"Failed to parse SCD transform SQL: $errorMsg")
        }
        LogicalPlanValidator.validate(logicalPlan) match {
          case Right(_) => logger.info("SCD transform LogicalPlan validation successful.")
          case Left(errors) => throw new RuntimeException(s"SCD transform LogicalPlan validation failed: ${errors.mkString("; ")}")
        }
        try { Dataset.ofRows(spark, logicalPlan) } catch {
          case e: Exception => throw new RuntimeException(s"Error executing SCD transform SQL (LogicalPlan): ${e.getMessage}", e)
        }
      case _ =>
        logger.info("No sql_logic provided for SCD source transformation. Using raw source data.")
        rawSourceDF
    }
    if (jobConfig.audit_level.toUpperCase != "NONE") {
        try { transformedSourceRecordsCount = Some(transformedSourceDF.count()) } catch { case e: Exception => logger.warn(s"Could not count transformed source records: ${e.getMessage}", e) }\n    }\n    logger.info(s"Transformed source data prepared. Records: ${transformedSourceRecordsCount.getOrElse("N/A")}")
    if (logger.isDebugEnabled) transformedSourceDF.printSchema()

    val targetDimensionPath = jobConfig.target_table_or_path
    val targetCurrentDF: DataFrame = try {
      logger.info(s"Reading current target dimension data from: $targetDimensionPath where ${scdCols.isCurrentCol} = true")
      val targetFullDF = spark.read.format(jobConfig.target_format.getOrElse("parquet")).load(targetDimensionPath)
      // Assuming isCurrentCol is Boolean type in target, adjust if String 'Y'/'N'
      targetFullDF.filter(col(scdCols.isCurrentCol) === true)
    } catch {
      case e: org.apache.spark.sql.AnalysisException if e.getMessage.toLowerCase.contains("path does not exist") =>
        logger.warn(s"Target dimension path $targetDimensionPath does not exist. Assuming initial load.")
        val scdSpecificCols = Seq(
            (scdCols.skIdCol, org.apache.spark.sql.types.LongType, false, org.apache.spark.sql.types.Metadata.empty),
            (scdCols.validFromCol, TimestampType, false, org.apache.spark.sql.types.Metadata.empty),
            (scdCols.validToCol, TimestampType, false, org.apache.spark.sql.types.Metadata.empty),
            (scdCols.versionCol, org.apache.spark.sql.types.IntegerType, false, org.apache.spark.sql.types.Metadata.empty),
            (scdCols.isCurrentCol, org.apache.spark.sql.types.BooleanType, false, org.apache.spark.sql.types.Metadata.empty),
            (scdCols.changeTypeCol, org.apache.spark.sql.types.StringType, true, org.apache.spark.sql.types.Metadata.empty)
        )
        val emptySchema = StructType(transformedSourceDF.schema.fields ++ scdSpecificCols.map(f => StructField(f._1, f._2, f._3, f._4)))
        spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], emptySchema)
      case e: Exception => throw new RuntimeException(s"Error reading target dimension table $targetDimensionPath: ${e.getMessage}", e)
    }
    if (jobConfig.audit_level.toUpperCase != "NONE") {
        try { targetCurrentRecordsCount = Some(targetCurrentDF.count()) } catch { case e: Exception => logger.warn(s"Could not count current target records: ${e.getMessage}", e) }\n    }\n    logger.info(s"Current target dimension data read. Active records: ${targetCurrentRecordsCount.getOrElse("N/A")}")

    val naturalKeyCols = jobConfig.scd2_natural_keys.getOrElse(throw new IllegalArgumentException("scd2_natural_keys must be defined for SCD2 jobs.")).split(',').map(_.trim).filter(_.nonEmpty)
    if (naturalKeyCols.isEmpty) throw new IllegalArgumentException("scd2_natural_keys cannot be empty for SCD2 jobs.")

    val sourceBusinessColsForHashing = transformedSourceDF.columns.filterNot(naturalKeyCols.contains).toSeq
    val categorizedRecords = Scd2LogicUtil.categorizeAndDetectChanges(
      transformedSourceDF,
      targetCurrentDF,
      naturalKeyCols,
      jobConfig.scd2_change_tracking_column,
      sourceBusinessColsForHashing,
      jobConfig.job_properties.flatMap(JSON.parseFull(_).collect{case m: Map[String,String] => m.get("scd2_handle_deletes_by_absence")}.flatten.map(_.toBoolean)).getOrElse(false),
      spark
    )

    val finalOutputDF = Scd2LogicUtil.generateScdOutputRecords(
      categorizedRecords,
      transformedSourceDF.columns.toSeq,
      targetCurrentDF.columns.toSeq, // Pass original target columns for selection of business cols from target
      naturalKeyCols,
      scdCols,
      scdProcessingTimestamp,
      scdHighDateTimestamp,
      spark
    )
    logger.info("Final SCD output records generated.")

    logger.info(s"Writing final output to target: ${jobConfig.target_table_or_path} using SaveMode.Append")
    val writer = finalOutputDF.write.mode(SaveMode.Append)
    jobConfig.target_format.foreach(format => writer.format(format.toLowerCase))
    jobConfig.target_format_options.foreach { optionsStr => JSON.parseFull(optionsStr) match { case Some(map: Map[String,String]) => writer.options(map) case _ => }}
    jobConfig.partitioning_columns.foreach { pc => val cols = pc.split(',').map(_.trim).filter(_.nonEmpty); if (cols.nonEmpty) writer.partitionBy(cols:_*) }
    writer.save(jobConfig.target_table_or_path)
    logger.info("Successfully wrote SCD records to target.")

    if (jobConfig.audit_level.toUpperCase != "NONE") {
        try { recordsWrittenCount = Some(finalOutputDF.count()) }
        catch { case e: Exception => logger.warn(s"Could not count final output records: ${e.getMessage}", e) }\n    }\n\n    // --- Perform Reconciliation for SCD2 --- \n    if (jobConfig.reconciliation_enabled.equalsIgnoreCase("Y") && jobConfig.reconciliation_config.exists(_.trim.nonEmpty)) {\n      logger.info("Starting reconciliation process for SCD2 handler...")\n      try {\n        // Reconcile the transformedSourceDF (desired current state) against the new current state in the target\n        val sourceForReconDF = transformedSourceDF \n        val targetFormatForRead = jobConfig.target_format.getOrElse("parquet")\n        logger.info(s"Re-reading target dimension from ${jobConfig.target_table_or_path} (format $targetFormatForRead) and filtering for current records for reconciliation.")\n        val currentTargetSnapshotDF = spark.read.format(targetFormatForRead).load(jobConfig.target_table_or_path).filter(col(scdCols.isCurrentCol) === true)\n        \n        val report = ReconciliationService.executeChecks(jobConfig.job_id, sourceForReconDF, currentTargetSnapshotDF, jobConfig.reconciliation_config.get, spark)\n        logger.info(s"Reconciliation Report for job ${jobConfig.job_id} (SCD2 - Current View): Status - ${report.overallStatus}")\n        report.checkResults.foreach(cr => logger.info(s"  Check: ${cr.checkType}, Status: ${cr.status}, SrcVal: ${cr.sourceValue}, TgtVal: ${cr.targetValue}, Diff: ${cr.difference.getOrElse("N/A")}, Msg: ${cr.message}"))\n        if (report.overallStatus != "PASS") {\n          logger.warn(s"Reconciliation for job ${jobConfig.job_id} (SCD2 - Current View) resulted in status: ${report.overallStatus}.")\n        }\n      } catch {\n        case e: Exception => \n          logger.error(s"Error during SCD2 reconciliation process for job ${jobConfig.job_id}: ${e.getMessage}", e)\n      }\n    }\n\n    logger.info(s"SCD Type 2 ingestion for job_id: ${jobConfig.job_id} completed successfully. Records affected/written: ${recordsWrittenCount.getOrElse("N/A")}")\n    (transformedSourceRecordsCount, recordsWrittenCount) \n  }\n}
