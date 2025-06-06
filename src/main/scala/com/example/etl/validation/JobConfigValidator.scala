package com.example.etl.validation

import com.example.etl.config.JobConfig
import com.example.etl.sql.SqlParserService
import com.example.etl.util.SchemaMapper
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON
import scala.util.Try

object JobConfigValidator {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  private val SupportedTransformationModes = Set("AS_IS", "WITH_LOGIC", "SCD2")
  private val SupportedLoadTypes = Set("INCREMENTAL", "FULL_RELOAD")
  private val SupportedWriteModes = Set("overwrite", "append", "ignore", "errorifexists")
  private val SupportedSourceTypes = Set("FILE", "HIVE_TABLE") // Added HIVE_TABLE
  private val SupportedTargetTypes = Set("FILE", "HDFS", "HIVE_TABLE") // Added HIVE_TABLE
  private val SupportedFileFormats = Set("CSV", "PARQUET", "JSON", "ORC", "TEXT", "AVRO")

  def validate(jobConfig: JobConfig, spark: SparkSession): List[String] = {
    var errors = List.empty[String]

    if (jobConfig.job_name == null || jobConfig.job_name.trim.isEmpty) errors :+= "job_name cannot be null or empty." // Changed from job_id
    // job_description is Option, no check needed unless specific rules apply
    if (jobConfig.is_active == null || !Seq("Y", "N").contains(jobConfig.is_active.toUpperCase)) errors :+= "is_active must be 'Y' or 'N'."

    if (jobConfig.source_type == null || jobConfig.source_type.trim.isEmpty) errors :+= "source_type cannot be null or empty."
    else if (!SupportedSourceTypes.contains(jobConfig.source_type.toUpperCase)) errors :+= s"Unsupported source_type: '${jobConfig.source_type}'. Supported: ${SupportedSourceTypes.mkString(", ")}"\n    if (jobConfig.target_type == null || jobConfig.target_type.trim.isEmpty) errors :+= "target_type cannot be null or empty."
    else if (!SupportedTargetTypes.contains(jobConfig.target_type.toUpperCase)) errors :+= s"Unsupported target_type: '${jobConfig.target_type}'. Supported: ${SupportedTargetTypes.mkString(", ")}"\n    if (jobConfig.target_table_or_path == null || jobConfig.target_table_or_path.trim.isEmpty) errors :+= "target_table_or_path cannot be null or empty."
    if (jobConfig.transformation_mode == null || jobConfig.transformation_mode.trim.isEmpty) errors :+= "transformation_mode cannot be null or empty."
    else if (!SupportedTransformationModes.contains(jobConfig.transformation_mode.toUpperCase)) errors :+= s"Unsupported transformation_mode: '${jobConfig.transformation_mode}'. Supported: ${SupportedTransformationModes.mkString(", ")}"\n    if (jobConfig.load_type == null || jobConfig.load_type.trim.isEmpty) errors :+= "load_type cannot be null or empty."
    else if (!SupportedLoadTypes.contains(jobConfig.load_type.toUpperCase)) errors :+= s"Unsupported load_type: '${jobConfig.load_type}'. Supported: ${SupportedLoadTypes.mkString(", ")}"\n    if (jobConfig.target_write_mode == null || jobConfig.target_write_mode.trim.isEmpty) errors :+= "target_write_mode cannot be null or empty."
    else if (!SupportedWriteModes.contains(jobConfig.target_write_mode.toLowerCase)) errors :+= s"Unsupported target_write_mode: '${jobConfig.target_write_mode}'. Supported: ${SupportedWriteModes.mkString(", ")}"\n\n    jobConfig.source_type.toUpperCase match {\n      case "FILE" =>\n        if (jobConfig.source_format.isEmpty || jobConfig.source_format.get.trim.isEmpty) errors :+= "source_format is mandatory for source_type 'FILE'."\n        else if (!SupportedFileFormats.contains(jobConfig.source_format.get.toUpperCase)) errors :+= s"Unsupported source_format: '${jobConfig.source_format.get}'. Check supported formats."\n        if (jobConfig.source_connection_details.isEmpty || jobConfig.source_connection_details.get.trim.isEmpty) errors :+= "source_connection_details (as path) is mandatory for source_type 'FILE'."\n      case "HIVE_TABLE" =>\n        if (jobConfig.source_connection_details.isEmpty || jobConfig.source_connection_details.get.trim.isEmpty) errors :+= "source_connection_details (as db.table) is mandatory for source_type 'HIVE_TABLE'."\n        if (jobConfig.source_format.isDefined) errors :+= "source_format should not be defined for source_type 'HIVE_TABLE'."\n      case _ => // Other types, or caught by SupportedSourceTypes check\n    }\n\n    jobConfig.target_type.toUpperCase match {\n      case "HIVE_TABLE" =>\n        if (jobConfig.target_format.isDefined && jobConfig.target_format.get.trim.nonEmpty) {\n            logger.warn("target_format is usually ignored when target_type is 'HIVE_TABLE' (uses Hive's underlying format), but proceeding.")\n        }\n      case _ => // Other types\n    }\n\n    jobConfig.transformation_mode.toUpperCase match {\n      case "WITH_LOGIC" | "SCD2" =>\n        if (jobConfig.sql_logic.isEmpty || jobConfig.sql_logic.get.trim.isEmpty) {\n          errors :+= s"sql_logic is mandatory for transformation_mode '${jobConfig.transformation_mode}'."\n        } \n      case _ => \n    }\n    if (jobConfig.transformation_mode.toUpperCase == "SCD2") {\n      if (jobConfig.scd2_natural_keys.isEmpty || jobConfig.scd2_natural_keys.get.trim.isEmpty) {\n        errors :+= "scd2_natural_keys is mandatory for transformation_mode 'SCD2'."\n      }\n    }\n\n    jobConfig.schema_mapping_logic.foreach { mappingJson =>\n      if (mappingJson.trim.nonEmpty) {\n        Try(SchemaMapper.parseMappingLogic(mappingJson)) match {\n          case Success(instructions) if mappingJson.trim.matches("\\s*\\{\\s*\\}\\s*") && instructions.nonEmpty => \n          case Success(instructions) if instructions.isEmpty && !mappingJson.trim.matches("\\s*\\{\\s*\\}\\s*") =>\n            errors :+= "schema_mapping_logic is provided but resulted in no valid mapping instructions."\n          case Failure(ex) =>\n            errors :+= s"schema_mapping_logic is not valid JSON: ${ex.getMessage}"\n          case _ => \n        }\n      }\n    }\n\n    jobConfig.job_properties.foreach { propsJson =>\n      if (propsJson.trim.nonEmpty) {\n        Try(JSON.parseFull(propsJson)) match {\n          case Success(Some(_: Map[_,_])) => \n          case Success(_) => errors :+= "job_properties is not a valid JSON object." \n          case Failure(ex) => errors :+= s"job_properties is not valid JSON: ${ex.getMessage}"\n        }\n      }\n    }\n\n    if (errors.nonEmpty) {\n      logger.error(s"Job configuration validation failed for job_name '${jobConfig.job_name}':\n - ${errors.mkString("\n - ")}")\n    }\n    errors\n  }\n}
