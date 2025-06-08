package com.example.etl.dq

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON
import scala.util.{Try, Success, Failure}

// --- Configuration Case Classes for dq_rules_config JSON ---
case class DQRuleConfig(
  rule_name: String,
  rule_type: String,
  columns: Option[List[String]] = None,
  expression: Option[String] = None,
  allowed_values: Option[List[Any]] = None,
  error_level: String = "WARN",
  description: Option[String] = None
)

case class DQRootConfig(
  rules: List[DQRuleConfig]
)

// --- Report Case Classes ---
case class DQRuleResult(
  rule_name: String,
  rule_type: String,
  status: String, // "PASS", "FAIL", "ERROR"
  failing_records_count: Option[Long] = None,
  message: String,
  sample_failing_records: Option[DataFrame] = None
)

case class DataQualityReport(
  job_name: String,
  overall_status: String,
  timestamp: String,
  rule_results: List[DQRuleResult]
) {
  def toJsonString: String = {
    val resultsJson = rule_results.map { r =>
      // Basic escape for message, proper JSON lib recommended for production
      val escapedMessage = JSONUtils.escape(r.message)
      s"{\"rule_name\":\"${JSONUtils.escape(r.rule_name)}\", \"rule_type\":\"${JSONUtils.escape(r.rule_type)}\", \"status\":\"${r.status}\", \"failing_records_count\":${r.failing_records_count.getOrElse("null")}, \"message\":\"$escapedMessage\"}"
    }.mkString("[",",","]")
    s"{\"job_name\":\"${JSONUtils.escape(job_name)}\", \"overall_status\":\"$overall_status\", \"timestamp\":\"$timestamp\", \"rule_results\":$resultsJson}"
  }
}

object JSONUtils {
  def escape(raw: String): String = {
    var escaped = raw; escaped = escaped.replace("\\", "\\\\"); escaped = escaped.replace("\"", "\\\""); escaped = escaped.replace("\b", "\\b"); escaped = escaped.replace("\f", "\\f"); escaped = escaped.replace("\n", "\\n"); escaped = escaped.replace("\r", "\\r"); escaped = escaped.replace("\t", "\\t"); escaped
  }
}

object DataQualityService {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  private val isoDateTimeFormat = java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME

  private def parseDqRules(rulesJson: String): Try[DQRootConfig] = Try {
    JSON.parseFull(rulesJson) match {
      case Some(parsed: Map[String, Any] @unchecked) =>
        val rulesList = parsed.get("rules") match {
          case Some(rules: List[Map[String, Any]] @unchecked) =>
            rules.map { ruleMap =>
              DQRuleConfig(
                rule_name = ruleMap.get("rule_name").collect { case s: String => s }.getOrElse(throw new IllegalArgumentException("Missing 'rule_name'")),
                rule_type = ruleMap.get("rule_type").collect { case s: String => s }.getOrElse(throw new IllegalArgumentException("Missing 'rule_type'")),
                columns = ruleMap.get("columns").collect { case l: List[String] @unchecked => l },
                expression = ruleMap.get("expression").collect { case s: String => s },
                allowed_values = ruleMap.get("allowed_values").collect { case l: List[Any] @unchecked => l },
                error_level = ruleMap.get("error_level").collect { case s: String => s.toUpperCase }.getOrElse("WARN"),
                description = ruleMap.get("description").collect { case s: String => s }\n              )\n            }\n          case _ => throw new IllegalArgumentException("'rules' array not found/list in dq_rules_config")\n        }\n        DQRootConfig(rulesList)\n      case _ => throw new IllegalArgumentException("dq_rules_config not a valid JSON object with 'rules' array")\n    }\n  }\n\n  def applyChecks(jobName: String, targetDF: DataFrame, rulesJson: String, spark: SparkSession): DataQualityReport = {\n    val reportTimestamp = java.time.LocalDateTime.now().format(isoDateTimeFormat)\n    logger.info(s"Starting Data Quality checks for job: $jobName at $reportTimestamp on DataFrame with columns: ${targetDF.columns.mkString(", ")}")\n\n    parseDqRules(rulesJson) match {\n      case Failure(ex) =>\n        logger.error(s"Failed to parse DQ rules JSON for job $jobName: ${ex.getMessage}", ex)\n        DataQualityReport(jobName, "ERROR", reportTimestamp, List(DQRuleResult("ConfigurationParse", "DQ_CONFIG_PARSE", "ERROR", None, s"Failed to parse DQ rules: ${ex.getMessage}")))\n      case Success(dqRootConfig) if dqRootConfig.rules.isEmpty =>\n        logger.info("No DQ rules configured. Skipping DQ checks.")\n        DataQualityReport(jobName, "PASS", reportTimestamp, List.empty)\n      case Success(dqRootConfig) =>\n        val ruleResults = dqRootConfig.rules.map { ruleConf =>\n          Try {\n            val (status, failingCount, message) = ruleConf.rule_type.toUpperCase match {\n              case "NOT_NULL" =>\n                val cols = ruleConf.columns.getOrElse(List.empty)\n                if (cols.isEmpty) throw new IllegalArgumentException("'columns' must be specified for NOT_NULL rule.")\n                val nullCheckCondition = cols.map(c => col(c).isNull).reduce(_ || _)\n                val failures = targetDF.filter(nullCheckCondition).count()\n                (if (failures == 0) "PASS" else "FAIL", Some(failures), s"Checked NOT NULL for columns: ${cols.mkString(", ")}. Found $failures null instances.")\n              \n              case "ROW_COUNT" =>\n                val rangeExpr = ruleConf.expression.getOrElse(throw new IllegalArgumentException("'expression' with min/max range is required for ROW_COUNT rule."))\n                val rangeMap = JSON.parseFull(rangeExpr).collect{case m: Map[String,Double] @unchecked => m}.getOrElse(throw new IllegalArgumentException("ROW_COUNT expression must be JSON like {\"min\":1, \"max\":100}"))\n                val minCount = rangeMap.get("min").map(_.toLong)\n                val maxCount = rangeMap.get("max").map(_.toLong)\n                val actualCount = targetDF.count()\n                var pass = true\n                var msg = s"Actual row count: $actualCount. Expected range: "\n                minCount.foreach(m => { pass &&= (actualCount >= m); msg += s"min=$m " })\n                maxCount.foreach(m => { pass &&= (actualCount <= m); msg += s"max=$m" })\n                if(minCount.isEmpty && maxCount.isEmpty) {pass=false; msg="Min/Max not defined in expression for ROW_COUNT."}\n                (if (pass) "PASS" else "FAIL", Some(if(pass) 0 else actualCount), msg)\n\n              case "UNIQUE_CHECK" =>\n                val cols = ruleConf.columns.getOrElse(List.empty)\n                if (cols.isEmpty) throw new IllegalArgumentException("'columns' must be specified for UNIQUE_CHECK rule.")\n                val duplicateGroups = targetDF.groupBy(cols.map(col):_*).count().filter(col("count") > 1)\n                val failingGroupsCount = duplicateGroups.count() // Number of groups with duplicates\n                // To count actual duplicate records (excluding one original): targetDF.except(targetDF.dropDuplicates(cols)).count() or sum(col("count")-1) from duplicateGroups\n                val approxFailingRecords = if(failingGroupsCount > 0) duplicateGroups.agg(sum(col("count") - 1)).first().getLong(0) else 0L\n                (if (failingGroupsCount == 0) "PASS" else "FAIL", Some(approxFailingRecords), s"Checked UNIQUE for columns: ${cols.mkString(", ")}. Found $failingGroupsCount groups with duplicates (approx $approxFailingRecords duplicate records).")\n\n              case "REGEX_MATCH" =>\n                val cols = ruleConf.columns.getOrElse(List.empty)\n                if (cols.isEmpty) throw new IllegalArgumentException("'columns' must be specified for REGEX_MATCH rule.")\n                val pattern = ruleConf.expression.getOrElse(throw new IllegalArgumentException("'expression' (regex pattern) is required for REGEX_MATCH rule."))\n                var totalFailures = 0L\n                cols.foreach(c => totalFailures += targetDF.filter(col(c).isNotNull && !col(c).rlike(pattern)).count())\n                (if (totalFailures == 0) "PASS" else "FAIL", Some(totalFailures), s"Checked REGEX_MATCH for columns: ${cols.mkString(", ")} with pattern '$pattern'. Found $totalFailures non-matching values.")\n\n              case "CUSTOM_SQL_EXPRESSION" =>\n                val sqlExpr = ruleConf.expression.getOrElse(throw new IllegalArgumentException("'expression' (SQL boolean condition) is required for CUSTOM_SQL_EXPRESSION rule."))\n                val failures = targetDF.filter(s"NOT ($sqlExpr)").count()\n                (if (failures == 0) "PASS" else "FAIL", Some(failures), s"Checked CUSTOM_SQL_EXPRESSION: '$sqlExpr'. Found $failures records failing the condition.")\n\n              // TODO: Implement ALLOWED_VALUES, BOUNDS_CHECK etc.\n              case other => \n                ( "ERROR", None, s"Unsupported DQ rule_type: '$other'")\n            }\n            DQRuleResult(ruleConf.rule_name, ruleConf.rule_type, status, failingCount, message)\n          } match {\n            case Success(result) => result\n            case Failure(ex) => \n              logger.error(s"Error executing DQ rule '${ruleConf.rule_name}': ${ex.getMessage}", ex)\n              DQRuleResult(ruleConf.rule_name, ruleConf.rule_type, "ERROR", None, s"Rule execution failed: ${ex.getMessage}")\n          }\n        }\n        val overallStatus = if (ruleResults.exists(_.status == "ERROR")) "ERROR" \n                          else if (ruleResults.exists(r => r.status == "FAIL" && dqRootConfig.rules.find(_.rule_name == r.rule_name).exists(_.error_level == "FAIL_JOB"))) "FAIL" \n                          else if (ruleResults.exists(_.status == "FAIL")) "WARN" // At least one FAIL (which must be WARN level by this point)\n                          else "PASS"\n        DataQualityReport(jobName, overallStatus, reportTimestamp, ruleResults)\n      }\n    }\n  }\n}
