package com.example.etl.logic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.log4j.Logger

object Scd2LogicUtil {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  private def createHash(df: DataFrame, colsToHash: Seq[String], hashColName: String): DataFrame = {
    if (colsToHash.isEmpty) {
      logger.warn(s"No columns specified for hashing to create column '$hashColName'. Adding a null column.")
      df.withColumn(hashColName, lit(null))
    } else {
      val existingCols = colsToHash.filter(df.columns.contains)
      if (existingCols.length != colsToHash.length) {
        val missing = colsToHash.diff(existingCols)
        logger.warn(s"Not all columns specified for hashing exist in DataFrame. Missing: ${missing.mkString(", ")}. Hashing only existing columns: ${existingCols.mkString(", ")}")
      }
      if (existingCols.isEmpty) {
          logger.warn(s"None of the specified hash columns exist. Adding a null '$hashColName' column.")
          return df.withColumn(hashColName, lit(null))
      }
      val concatenatedCols = existingCols.map(c => coalesce(col(c).cast("string"), lit("")))\n      df.withColumn(hashColName, md5(concat_ws("|", concatenatedCols:_*)))
    }
  }

  case class CategorizedRecords(
    newRecordsDF: DataFrame,
    matchedSourceRecordsDF: DataFrame,
    logicallyDeletedTargetRecordsDF: DataFrame,
    unchangedMatchedTargetRecordsDF: DataFrame
  )

  def categorizeAndDetectChanges(
    sourceDF: DataFrame,
    targetCurrentDF: DataFrame,
    naturalKeyCols: Seq[String],
    scd2ChangeTrackingCol: Option[String],
    businessColsForHashing: Seq[String],
    handleDeletesByAbsence: Boolean,
    spark: SparkSession
  ): CategorizedRecords = {
    val targetPrefixedDF = targetCurrentDF.columns.foldLeft(targetCurrentDF)((df, c) => df.withColumnRenamed(c, s"target_${c}"))
    val newRecordsDF = sourceDF.join(targetPrefixedDF, naturalKeyCols.map(col), "left_anti")
    logger.info(s"Identified ${newRecordsDF.count()} new records.")
    val joinedDF = sourceDF.join(targetPrefixedDF, naturalKeyCols.map(col), "inner")
    logger.info(s"Found ${joinedDF.count()} records in source matching natural keys in target.")
    val recordsWithChangeFlagDF = scd2ChangeTrackingCol match {
      case Some(trackingCol) =>
        if (!joinedDF.columns.contains(trackingCol) || !joinedDF.columns.contains(s"target_${trackingCol}")) {
            val errMsg = s"Change tracking column '$trackingCol' not found in joined source/target. Source cols: ${sourceDF.columns.mkString(",")}, Target cols: ${targetCurrentDF.columns.mkString(",")}"
            logger.error(errMsg)
            throw new IllegalArgumentException(errMsg)
        }
        joinedDF.withColumn("has_changed", coalesce(col(trackingCol) <=> col(s"target_${trackingCol}"), lit(true)).not)
      case None =>
        logger.info(s"No scd2ChangeTrackingCol provided. Using hash-based change detection on columns: ${businessColsForHashing.mkString(", ")}")
        val sourceHashed = createHash(joinedDF, businessColsForHashing, "source_hash")
        val targetBusinessColsForHashing = businessColsForHashing.map(c => s"target_${c}")
        val sourceAndTargetHashed = createHash(sourceHashed, targetBusinessColsForHashing, "target_hash")
        sourceAndTargetHashed.withColumn("has_changed", col("source_hash") =!= col("target_hash"))
    }
    val finalMatchedSourceRecordsDF = joinedDF.join(recordsWithChangeFlagDF.select((naturalKeyCols :+ "has_changed").map(col):_*), naturalKeyCols, "inner")
    val logicallyDeletedTargetRecordsDF = if (handleDeletesByAbsence) {
      val df = targetPrefixedDF.join(sourceDF, naturalKeyCols.map(c => targetPrefixedDF(s"target_${c}") === sourceDF(c)), "left_anti")
      logger.info(s"Identified ${df.count()} logically deleted records (target records not in source).")
      df
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], targetPrefixedDF.schema)
    }
    val unchangedSourceRecordsDF = recordsWithChangeFlagDF.filter(col("has_changed") === false)
    val unchangedTargetNaturalKeysDF = unchangedSourceRecordsDF.select(naturalKeyCols.map(col):_*).distinct()
    val finalUnchangedMatchedTargetRecordsDF = targetPrefixedDF.join(unchangedTargetNaturalKeysDF, naturalKeyCols.map(c => targetPrefixedDF(s"target_${c}") === unchangedTargetNaturalKeysDF(c)), "inner")
    logger.info(s"Identified ${finalUnchangedMatchedTargetRecordsDF.count()} unchanged matched target records.")
    CategorizedRecords(newRecordsDF, finalMatchedSourceRecordsDF, logicallyDeletedTargetRecordsDF, finalUnchangedMatchedTargetRecordsDF)
  }

  // Define simple case class for SCD column names for now, can be replaced by JobConfig fields later
  case class ScdColumnNames(
    skIdCol: String = "sk_id",
    validFromCol: String = "valid_from_ts",
    validToCol: String = "valid_to_ts",
    versionCol: String = "version",
    isCurrentCol: String = "is_current",
    changeTypeCol: String = "change_type"
  )

  def generateScdOutputRecords(
    categorizedRecords: CategorizedRecords,
    sourceBusinessCols: Seq[String], // All business columns from the original source schema
    targetBusinessColsWithPrefix: Seq[String], // All business columns from target, with target_ prefix
    naturalKeyCols: Seq[String], // Names of natural key columns (no prefix)
    scdCols: ScdColumnNames,
    processingTimestamp: org.apache.spark.sql.Timestamp,
    highDateTimestamp: org.apache.spark.sql.Timestamp,
    spark: SparkSession
  ): DataFrame = {

    val scdProcessingTimestamp = lit(processingTimestamp).cast(TimestampType)
    val scdHighDateTimestamp = lit(highDateTimestamp).cast(TimestampType)

    // 1. New Records (Type: I)
    val newRecordsOutputDF = categorizedRecords.newRecordsDF
      .select(sourceBusinessCols.map(col):_*)
      .withColumn(scdCols.skIdCol, lit(null)) // SK to be generated later
      .withColumn(scdCols.validFromCol, scdProcessingTimestamp)
      .withColumn(scdCols.validToCol, scdHighDateTimestamp)
      .withColumn(scdCols.versionCol, lit(1))
      .withColumn(scdCols.isCurrentCol, lit(true))
      .withColumn(scdCols.changeTypeCol, lit("I"))

    val changedRecordsInputDF = categorizedRecords.matchedSourceRecordsDF.filter(col("has_changed") === true)

    // 2a. Expired Records from Updates (Type: U_EXPIRED) - Retain original SK
    val expiredFromUpdateOutputDF = changedRecordsInputDF
      .select(targetBusinessColsWithPrefix.map(c => col(c).alias(c.replaceFirst("target_", ""))):+ col(s"target_${scdCols.skIdCol}").alias(scdCols.skIdCol) :+ col(s"target_${scdCols.versionCol}").alias(scdCols.versionCol):+ col(s"target_${scdCols.validFromCol}").alias(scdCols.validFromCol) :_*)
      .withColumn(scdCols.validToCol, scdProcessingTimestamp)
      .withColumn(scdCols.isCurrentCol, lit(false))
      .withColumn(scdCols.changeTypeCol, lit("U_EXPIRED"))
      // Ensure all SCD columns are present, even if just carrying over old version/valid_from
      .select((sourceBusinessCols ++ Seq(scdCols.skIdCol, scdCols.validFromCol, scdCols.validToCol, scdCols.versionCol, scdCols.isCurrentCol, scdCols.changeTypeCol)).map(c => col(c)):_*)

    // 2b. New Current Records from Updates (Type: I) - New SK needed
    val newCurrentFromUpdateOutputDF = changedRecordsInputDF
      .select(sourceBusinessCols.map(col):_* :+ (col(s"target_${scdCols.versionCol}") + 1).alias(scdCols.versionCol) :_*)
      .withColumn(scdCols.skIdCol, lit(null)) // SK to be generated later
      .withColumn(scdCols.validFromCol, scdProcessingTimestamp)
      .withColumn(scdCols.validToCol, scdHighDateTimestamp)
      .withColumn(scdCols.isCurrentCol, lit(true))
      .withColumn(scdCols.changeTypeCol, lit("I"))

    // 3. Logically Deleted Records (Type: D) - Retain original SK
    val logicallyDeletedOutputDF = categorizedRecords.logicallyDeletedTargetRecordsDF
      .select(targetBusinessColsWithPrefix.map(c => col(c).alias(c.replaceFirst("target_", ""))):+ col(s"target_${scdCols.skIdCol}").alias(scdCols.skIdCol) :+ col(s"target_${scdCols.versionCol}").alias(scdCols.versionCol):+ col(s"target_${scdCols.validFromCol}").alias(scdCols.validFromCol) :_*)
      .withColumn(scdCols.validToCol, scdProcessingTimestamp)
      .withColumn(scdCols.isCurrentCol, lit(false))
      .withColumn(scdCols.changeTypeCol, lit("D"))
      // Ensure all SCD columns are present
      .select((sourceBusinessCols ++ Seq(scdCols.skIdCol, scdCols.validFromCol, scdCols.validToCol, scdCols.versionCol, scdCols.isCurrentCol, scdCols.changeTypeCol)).map(c => col(c)):_*)\n\n    // Union records that retain SKs and those that need new SKs\n    val recordsWithExistingSk = expiredFromUpdateOutputDF.unionByName(logicallyDeletedOutputDF, allowMissingColumns=true)\n    val recordsNeedingNewSk = newRecordsOutputDF.unionByName(newCurrentFromUpdateOutputDF, allowMissingColumns=true)\n\n    // Generate SKs for those that need them\n    // A more robust way for max SK would be to read from target, but for now, assume SKs are globally unique if generated by monotonically_increasing_id\n    val recordsWithNewSkGenerated = recordsNeedingNewSk\n      .withColumn("generated_sk_id", monotonically_increasing_id())\n      .drop(scdCols.skIdCol)\n      .withColumnRenamed("generated_sk_id", scdCols.skIdCol)\n      \n    // Final Union\n    // Ensure schemas are aligned before union if not using unionByName with allowMissingColumns extensively\n    // For safety, explicitly select columns in the desired final order for both DataFrames before union.\n    val finalSchemaCols = sourceBusinessCols ++ Seq(scdCols.skIdCol, scdCols.validFromCol, scdCols.validToCol, scdCols.versionCol, scdCols.isCurrentCol, scdCols.changeTypeCol)\n\n    val finalDf = recordsWithExistingSk.select(finalSchemaCols.map(c => col(c)):_*)\n      .unionByName(recordsWithNewSkGenerated.select(finalSchemaCols.map(c => col(c)):_*), allowMissingColumns=true) // allowMissingColumns might be needed if some paths don't generate all cols perfectly (e.g. version for new)\n\n    finalDf\n  }\n}
