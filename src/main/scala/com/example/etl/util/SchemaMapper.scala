package com.example.etl.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON
import scala.util.{Try, Success, Failure}

object SchemaMapper {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  case class ColumnMappingInstruction(
    sourceColumn: Option[String],
    targetName: String,
    targetType: Option[DataType] = None,
    defaultIfNull: Option[Any] = None,
    defaultValue: Option[Any] = None,
    dateFormat: Option[String] = None
  )

  private def parseDataType(typeStr: String): Option[DataType] = typeStr.toLowerCase match {
    case "string" => Some(StringType)
    case "integer" | "int" => Some(IntegerType)
    case "long" => Some(LongType)
    case "double" => Some(DoubleType)
    case "float" => Some(FloatType)
    case "boolean" => Some(BooleanType)
    case "date" => Some(DateType)
    case "timestamp" => Some(TimestampType)
    case "decimal" | "numeric" => Some(DecimalType.SYSTEM_DEFAULT)
    case s if s.startsWith("decimal(") && s.endsWith(")") => Try {
        val params = s.substring("decimal(".length, s.length - 1).split(",")
        if (params.length == 2) DecimalType(params(0).trim.toInt, params(1).trim.toInt) else DecimalType.SYSTEM_DEFAULT
    }.getOrElse(DecimalType.SYSTEM_DEFAULT)
    case _ => logger.warn(s"Unsupported target_type: '$typeStr'."); None
  }

  def parseMappingLogic(mappingJsonString: String): Seq[ColumnMappingInstruction] = {
    JSON.parseFull(mappingJsonString) match {
      case Some(map: Map[String, Any]) =>
        map.flatMap { case (sourceColOrPlaceholder, mappingValue) =>
          val instructionOpt: Option[ColumnMappingInstruction] = mappingValue match {
            case targetColNameStr: String =>
              Some(ColumnMappingInstruction(sourceColumn = Some(sourceColOrPlaceholder), targetName = targetColNameStr))
            case specMap: Map[String, Any] @unchecked =>
              val targetNameOpt = specMap.get("target_name").collect{case tn: String => tn}
              if (targetNameOpt.isEmpty) { logger.error(s"Missing 'target_name' in mapping for '$sourceColOrPlaceholder'. Skipping."); None }
              else {
                val targetName = targetNameOpt.get
                val targetTypeOpt = specMap.get("target_type").collect { case ttStr: String => parseDataType(ttStr) }.flatten
                val defaultIfNullOpt = specMap.get("default_if_null")
                val defaultValueOpt = specMap.get("default_value")
                val dateFormatOpt = specMap.get("date_format").collect { case df: String => df }
                // Determine if this is a new column based on defaultValue and potentially a placeholder convention
                val finalSourceCol = if(specMap.contains("source_column")) specMap("source_column").asInstanceOf[Option[String]] else Some(sourceColOrPlaceholder)
                                     // A bit more explicit: if defaultValue is present and sourceColumn is NOT, it's a new column from defaultValue
                val actualSourceColumn = if (defaultValueOpt.isDefined && finalSourceCol.getOrElse("") == sourceColOrPlaceholder && !inputSourceColumnsContain(sourceColOrPlaceholder, specMap)) None else finalSourceCol

                Some(ColumnMappingInstruction(
                  sourceColumn = actualSourceColumn, // Correctly make it None if it's purely a new col from default_value
                  targetName = targetName,
                  targetType = targetTypeOpt,
                  defaultIfNull = defaultIfNullOpt,
                  defaultValue = defaultValueOpt,
                  dateFormat = dateFormatOpt
                ))
              }
            case _ => logger.warn(s"Invalid mapping for '$sourceColOrPlaceholder'."); None
          }
          instructionOpt.toSeq
        }.toSeq
      case _ => logger.error(s"Could not parse schema_mapping_logic JSON: $mappingJsonString"); Seq.empty
    }
  }
  // Helper for parseMappingLogic to check if sourceColOrPlaceholder is likely a real source column name
  private def inputSourceColumnsContain(placeholder: String, spec:Map[String,Any]): Boolean = {
      // This is tricky without the actual DF columns. Assume for now that if 'source_column' is explicitly in spec, it's the authority.
      // If not, placeholder is assumed to be a source column unless it starts with a known 'new column' prefix.
      !placeholder.startsWith("__NEW_COLUMN__") // Example convention for explicit new columns
  }

  def applyMapping(
    df: DataFrame,
    instructions: Seq[ColumnMappingInstruction],
    spark: SparkSession,
    newSourceColumnBehavior: String = "ignore" // "ignore", "include", "fail"
  ): DataFrame = {
    val finalSelectCols = scala.collection.mutable.ListBuffer[org.apache.spark.sql.Column]()
    val inputColNamesOriginalCase = df.columns
    val inputColNamesLower = inputColNamesOriginalCase.map(_.toLowerCase).toSet
    val mappedOrProcessedTargetCols = scala.collection.mutable.Set[String]() // Tracks target names generated by instructions

    instructions.foreach { instruction =>
      var currentExpr: org.apache.spark.sql.Column = null
      var sourceColUsed: Option[String] = None

      instruction.sourceColumn match {
        case Some(srcColName) =>
          inputColNamesOriginalCase.find(_.equalsIgnoreCase(srcColName)) match {
            case Some(actualSrcColName) => // Found in input DF, preserve original casing
              currentExpr = col(actualSrcColName)
              sourceColUsed = Some(actualSrcColName)
            case None => // Source column in mapping not in DataFrame
              if (instruction.defaultValue.isDefined) {
                logger.warn(s"Source column '$srcColName' not found. Using defaultValue for target '${instruction.targetName}'.")
                currentExpr = lit(instruction.defaultValue.get)
              } else if (instruction.defaultIfNull.isDefined) {
                logger.warn(s"Source column '$srcColName' not found. Using defaultIfNull for target '${instruction.targetName}'.")
                currentExpr = lit(instruction.defaultIfNull.get)
              } else {
                logger.warn(s"Source column '$srcColName' not found. Target column '${instruction.targetName}' will be null.")
                currentExpr = lit(null).cast(instruction.targetType.getOrElse(StringType)) // Cast null to targetType if known, else StringType
              }
          }
        case None => // No source column defined - new column with defaultValue or defaultIfNull
          if (instruction.defaultValue.isDefined) {
            currentExpr = lit(instruction.defaultValue.get)
          } else if (instruction.defaultIfNull.isDefined) {
            currentExpr = lit(instruction.defaultIfNull.get) // Will be null if defaultIfNull is None, then cast
            instruction.targetType.foreach(t => currentExpr = currentExpr.cast(t)) // Cast if type known
          }else {
            logger.warn(s"Instruction for target '${instruction.targetName}' has no sourceColumn and no defaultValue/defaultIfNull. Will be null.")
            currentExpr = lit(null).cast(instruction.targetType.getOrElse(StringType))\n          }\n      }\n      \n      instruction.targetType.foreach { dtype =>\n        val currentExprDataType = Try(currentExpr.expr.dataType).getOrElse(NullType) \n        if (dtype == DateType && instruction.dateFormat.isDefined && currentExprDataType == StringType) {\n          currentExpr = to_date(currentExpr, instruction.dateFormat.get)\n        } else if (dtype == TimestampType && instruction.dateFormat.isDefined && currentExprDataType == StringType) {\n          currentExpr = to_timestamp(currentExpr, instruction.dateFormat.get)\n        } else if (currentExprDataType != dtype) { // Avoid redundant cast\n          currentExpr = currentExpr.cast(dtype)\n        }\n      }\n\n      instruction.defaultIfNull.foreach { defaultVal =>\n        val targetDataType = currentExpr.expr.dataType\n        currentExpr = coalesce(currentExpr, lit(defaultVal).cast(targetDataType))\n      }\n\n      if (instruction.sourceColumn.isDefined && instruction.defaultValue.isDefined) {\n         logger.warn(s"Applying defaultValue for '${instruction.targetName}', overwriting current expression derived from '${instruction.sourceColumn.get}'.")\n         currentExpr = lit(instruction.defaultValue.get)\n         instruction.targetType.foreach { dtype => currentExpr = currentExpr.cast(dtype) } \n      }\n      \n      finalSelectCols += currentExpr.alias(instruction.targetName)\n      mappedOrProcessedTargetCols.add(instruction.targetName.toLowerCase)\n    }\n\n    // Schema Drift Handling for unmapped source columns\n    val unmappedSourceCols = inputColNamesOriginalCase.filterNot(c => \n        instructions.exists(instr => instr.sourceColumn.exists(_.equalsIgnoreCase(c))) || \n        mappedOrProcessedTargetCols.contains(c.toLowerCase) // Also exclude if source name clashes with a target name already processed\n    )\n    \n    newSourceColumnBehavior.toLowerCase match {\n      case "include" =>\n        if (unmappedSourceCols.nonEmpty) logger.info(s"Including unmapped source columns: ${unmappedSourceCols.mkString(", ")}")\n        unmappedSourceCols.foreach(colName => {\n            // Check if this unmapped source column name conflicts with any target name already created\n            if (!mappedOrProcessedTargetCols.contains(colName.toLowerCase)) {\n                finalSelectCols += col(colName) // Add as is\n            } else {\n                logger.warn(s"Unmapped source column '$colName' conflicts with an existing target column name. It will be excluded.")\n            }\n        })\n      case "fail" =>\n        if (unmappedSourceCols.nonEmpty) {\n          val errMsg = s"Job failed due to unmapped source columns: ${unmappedSourceCols.mkString(", ")}. Behavior set to 'fail'."\n          logger.error(errMsg)\n          throw new RuntimeException(errMsg)\n        }\n      case "ignore" =>\n        if (unmappedSourceCols.nonEmpty) logger.info(s"Ignoring unmapped source columns: ${unmappedSourceCols.mkString(", ")}")\n      case behavior => \n        logger.warn(s"Invalid newSourceColumnBehavior: '$behavior'. Defaulting to 'ignore'.")\n        if (unmappedSourceCols.nonEmpty) logger.info(s"Ignoring unmapped source columns (due to invalid behavior): ${unmappedSourceCols.mkString(", ")}")\n    }\n\n    if (finalSelectCols.isEmpty) {\n        logger.warn("Schema mapping and drift handling resulted in an empty column set. Returning an empty DataFrame.")\n        spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], StructType(Nil))\n    } else {\n        df.select(finalSelectCols:_*)\n    }\n  }\n}
