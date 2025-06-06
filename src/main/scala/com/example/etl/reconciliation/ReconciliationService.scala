package com.example.etl.reconciliation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON
import scala.util.{Try, Success, Failure}

// --- Configuration Case Classes ---
case class ReconciliationCheckConfig(
  `type`: String,
  columns: Option[List[String]] = None,
  tolerance_percent: Option[Double] = None,
  tolerance_absolute: Option[Double] = None,
  hash_function: Option[String] = None,
  source_alias: Option[String] = None,
  target_alias: Option[String] = None
)

case class ReconciliationRootConfig(
  checks: List[ReconciliationCheckConfig]
)

// --- Report Case Classes ---
case class CheckResult(
  checkType: String,
  status: String,
  sourceValue: String,
  targetValue: String,
  difference: Option[String] = None,
  message: String
)

case class ReconciliationReport(
  jobName: String, // Changed from jobId
  overallStatus: String,
  checkResults: List[CheckResult]
)

object ReconciliationService {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  private def mapToJsonString(map: Map[String, Any]): String = {
    map.map { case (k, v) => s"\"$k\": ${if (v.isInstanceOf[String]) s"\"$v\"" else v.toString}" }.mkString("{ ", ", ", " }")
  }

  private def parseConfig(configJson: String): Try[ReconciliationRootConfig] = Try {
    JSON.parseFull(configJson) match {
      case Some(parsed: Map[String, Any] @unchecked) =>
        val checksList = parsed.get("checks") match {
          case Some(checks: List[Map[String, Any]] @unchecked) =>
            checks.map { checkMap =>
              ReconciliationCheckConfig(
                `type` = checkMap.get("type").collect { case s: String => s }.getOrElse(throw new IllegalArgumentException("Missing 'type' in check config")),
                columns = checkMap.get("columns").collect { case l: List[String] @unchecked => l },
                tolerance_percent = checkMap.get("tolerance_percent").collect { case d: Double => d },
                tolerance_absolute = checkMap.get("tolerance_absolute").collect { case d: Double => d },
                hash_function = checkMap.get("hash_function").collect { case s: String => s },
                source_alias = checkMap.get("source_alias").collect { case s: String => s },
                target_alias = checkMap.get("target_alias").collect { case s: String => s }\n              )\n            }\n          case _ => throw new IllegalArgumentException("'checks' array not found or not a list in reconciliation_config")\n        }\n        ReconciliationRootConfig(checksList)\n      case _ => throw new IllegalArgumentException("reconciliation_config is not a valid JSON object")\n    }\n  }\n\n  // Changed jobId to jobName in signature\n  def executeChecks(jobName: String, sourceDF: DataFrame, targetDF: DataFrame, configJson: String, spark: SparkSession): ReconciliationReport = {\n    val parseResult = parseConfig(configJson)\n    if (parseResult.isFailure) {\n      val errMsg = s"Failed to parse reconciliation_config JSON for job $jobName: ${parseResult.failed.get.getMessage}"\n      logger.error(errMsg, parseResult.failed.get)\n      return ReconciliationReport(jobName, "ERROR", List(CheckResult("ConfigurationParse", "ERROR", "N/A", "N/A", message = errMsg)))\n    }\n\n    val rootConfig = parseResult.get\n    var overallChecksPass = true\n    val results = rootConfig.checks.map { checkConf =>\n      Try {\n        checkConf.`type`.toLowerCase match {\n          case "record_count" =>\n            val sourceCount = sourceDF.count()\n            val targetCount = targetDF.count()\n            val diff = Math.abs(sourceCount - targetCount)\n            val percentDiff = if (sourceCount == 0 && targetCount == 0) 0.0 else if (sourceCount == 0) 100.0 else (diff.toDouble / sourceCount) * 100.0\n            val tolerance = checkConf.tolerance_percent.getOrElse(0.0)\n            val pass = percentDiff <= tolerance\n            if (!pass) overallChecksPass = false\n            CheckResult(checkConf.`type`, if (pass) "PASS" else "FAIL", sourceCount.toString, targetCount.toString, Some(s"Count diff: $diff, Percent diff: ${"%.2f".format(percentDiff)}%"), s"Record count comparison. Tolerance: ${tolerance}%")\n          \n          case "checksum" =>\n            val colsToCheck = checkConf.columns.getOrElse(List.empty)\n            if (colsToCheck.isEmpty) throw new IllegalArgumentException("Checksum check type requires a non-empty 'columns' list.")\n            val actualSourceCols = colsToCheck.filter(sourceDF.columns.contains)\n            val actualTargetCols = colsToCheck.filter(targetDF.columns.contains)\n            if (actualSourceCols.length != colsToCheck.length || actualTargetCols.length != colsToCheck.length) {\n                val missingSrc = colsToCheck.diff(actualSourceCols).mkString(", ")\n                val missingTgt = colsToCheck.diff(actualTargetCols).mkString(", ")\n                throw new IllegalArgumentException(s"Checksum columns missing for job $jobName. From source: [$missingSrc]. From target: [$missingTgt]")\n            }\n            val sourceChecksums = actualSourceCols.map(c => c -> Try(sourceDF.agg(sum(crc32(coalesce(col(c).cast("string"), lit(""))))).first().getAs[Long](0)).getOrElse(0L)).toMap\n            val targetChecksums = actualTargetCols.map(c => c -> Try(targetDF.agg(sum(crc32(coalesce(col(c).cast("string"), lit(""))))).first().getAs[Long](0)).getOrElse(0L)).toMap\n            val pass = sourceChecksums == targetChecksums\n            if (!pass) overallChecksPass = false\n            val diffMsg = if (pass) "Checks passed." else actualSourceCols.filter(c => sourceChecksums.get(c) != targetChecksums.get(c)).map(c => s"$c(src:${sourceChecksums.get(c).getOrElse("N/A")},tgt:${targetChecksums.get(c).getOrElse("N/A")})").mkString("Mismatches: ", ", ", "")\n            CheckResult(checkConf.`type`, if (pass) "PASS" else "FAIL", mapToJsonString(sourceChecksums), mapToJsonString(targetChecksums), Some(diffMsg) ,s"CRC32 Sum checksum on columns: ${actualSourceCols.mkString(", ")}")\n\n          case "column_sum" =>\n            val colsToSum = checkConf.columns.getOrElse(List.empty)\n            if (colsToSum.isEmpty) throw new IllegalArgumentException("Column_sum check type requires a non-empty 'columns' list.")\n            var columnChecksPass = true\n            val sourceSums = colsToSum.map { c => \n                c -> (if (sourceDF.columns.contains(c)) Try(sourceDF.agg(sum(col(c).cast("double"))).first().getDouble(0)).getOrElse(0.0) else 0.0) \n            }.toMap\n            val targetSums = colsToSum.map { c => \n                c -> (if (targetDF.columns.contains(c)) Try(targetDF.agg(sum(col(c).cast("double"))).first().getDouble(0)).getOrElse(0.0) else 0.0) \n            }.toMap\n            val diffMessages = scala.collection.mutable.ListBuffer[String]()\n            colsToSum.foreach { colNameVal => \n              val srcSum = sourceSums.getOrElse(colNameVal, 0.0)\n              val tgtSum = targetSums.getOrElse(colNameVal, 0.0)\n              val absDiff = Math.abs(srcSum - tgtSum)\n              val pcntDiff = if (srcSum == 0) (if(tgtSum == 0) 0.0 else 100.0) else (absDiff / Math.abs(srcSum)) * 100\n              val absTol = checkConf.tolerance_absolute.getOrElse(Double.MaxValue)\n              val pcntTol = checkConf.tolerance_percent.getOrElse(Double.MaxValue)\n              val colPass = (absDiff <= absTol) || (pcntDiff <= pcntTol) \n              if (!colPass) {\n                columnChecksPass = false\n                diffMessages += s"Col '$colNameVal': srcSum=${srcSum}, tgtSum=${tgtSum} (absDiff=${"%.4f".format(absDiff)}} vs tolAbs=${absTol}; pcntDiff=${"%.2f".format(pcntDiff)}}% vs tolPcnt=${pcntTol}%)"\n              }\n            }\n            if (!columnChecksPass) overallChecksPass = false\n            CheckResult(checkConf.`type`, if (columnChecksPass) "PASS" else "FAIL", mapToJsonString(sourceSums), mapToJsonString(targetSums), if(diffMessages.isEmpty) None else Some(diffMessages.mkString("; ")), s"Sum comparison for columns: ${colsToSum.mkString(", ")}. Tolerances: abs=${checkConf.tolerance_absolute.getOrElse("N/A")}, pcnt=${checkConf.tolerance_percent.getOrElse("N/A")}")\n\n          case other =>\n            logger.warn(s"Unsupported reconciliation check type for job $jobName: '$other'")\n            overallChecksPass = false\n            CheckResult(other, "ERROR", "N/A", "N/A", message = s"Unsupported reconciliation check type: '$other'")\n        }\n      } match {\n        case Success(result) => result\n        case Failure(ex) => \n          logger.error(s"Error executing reconciliation check type '${checkConf.`type`}' for job $jobName: ${ex.getMessage}", ex)\n          overallChecksPass = false\n          CheckResult(checkConf.`type`, "ERROR", "N/A", "N/A", message = s"Execution error: ${ex.getMessage}")\n      }\n    }\n    ReconciliationReport(jobName, if (overallChecksPass && !results.exists(_.status == "ERROR")) "PASS" else "FAIL", results)\n  }\n}
