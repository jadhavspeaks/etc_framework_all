package com.example.etl.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.parser.ParseException // Specific Spark parser exception

object SqlParserService {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   * Parses a SQL string into Spark's LogicalPlan representation.
   *
   * @param sql The SQL query string to parse.
   * @param spark The active SparkSession, used to access the SQL parser.
   * @return Either a String containing an error message on failure (e.g., syntax errors),
   *         or Spark's LogicalPlan on success.
   */
  def parse(sql: String, spark: SparkSession): Either[String, LogicalPlan] = {
    if (sql == null || sql.trim.isEmpty) {
      return Left("SQL query cannot be null or empty.")
    }
    logger.info(s"Attempting to parse SQL using Spark's parser (first 100 chars): ${sql.take(100)}...")

    try {
      // Access Spark's session-specific SQL parser and parse the plan
      // This does not execute the query, only parses it into a logical plan
      val logicalPlan: LogicalPlan = spark.sessionState.sqlParser.parsePlan(sql)
      logger.info("SQL parsed successfully into Spark LogicalPlan.")
      Right(logicalPlan)
    } catch {
      case e: ParseException => // Spark's specific parsing exception
        logger.error(s"Spark SQL Parsing Error for query [${sql.take(200)}...]: ${e.getMessage}", e)
        // The ParseException often contains useful context like line and column, though accessing it directly might depend on Spark version details.
        // e.getMessage usually includes this.
        Left(s"Spark SQL Syntax Error: ${e.getMessage}")
      case e: Exception => // Catch-all for other unexpected errors during parsing
        logger.error(s"An unexpected error occurred during Spark SQL parsing for query [${sql.take(200)}...]: ${e.getMessage}", e)
        Left(s"Unexpected Spark SQL Parsing Error: ${e.getMessage}")
    }
  }

  // Optional: Example usage for quick testing (can be removed or moved to tests)
  /*
  def main(args: Array[String]): Unit = {
    // This main method would require a SparkSession to run,
    // so it's more of a conceptual test here or would need setup.
    // val spark = SparkSession.builder.master("local").appName("SqlParserTest").getOrCreate()

    // val goodSql = "SELECT a, b FROM my_table WHERE c > 10 ORDER BY a DESC"
    // val badSyntaxSql = "SELEC a, b FROM my_table"

    // Assuming spark is available:
    // parse(goodSql, spark) match {
    //   case Right(plan) => println(s"Good SQL parsed successfully:\n${plan.treeString}")
    //   case Left(error) => println(s"Error in good SQL (should not happen): $error")
    // }\n
    // parse(badSyntaxSql, spark) match {
    //   case Right(plan) => println(s"Error in bad SQL (should not happen):\n${plan.treeString}")
    //   case Left(error) => println(s"Bad SQL syntax error (expected): $error")
    // }\n
    // spark.stop()\n  }\n  */
}
