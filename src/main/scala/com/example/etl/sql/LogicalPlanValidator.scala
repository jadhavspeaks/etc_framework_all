package com.example.etl.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical._
// It might be necessary to import specific DDL/DML command nodes if they have unique classes
// For example: import org.apache.spark.sql.catalyst.analysis.CreateTableStatement
// Exact imports might vary slightly based on Spark version and specific commands to check.

object LogicalPlanValidator {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   * Validates a given Spark LogicalPlan against a set of predefined rules.
   * For now, it primarily checks for and disallows DDL/DML operations within
   * transformation logic.
   *
   * @param plan The LogicalPlan to validate.
   * @return Either a List of error messages if validation fails,
   *         or Unit if validation passes.
   */
  def validate(plan: LogicalPlan): Either[List[String], Unit] = {
    logger.info(s"Validating LogicalPlan:\n${plan.treeString}")
    var validationErrors: List[String] = List()

    // Recursive function to traverse the plan tree
    def checkNode(currentNode: LogicalPlan): Unit = {
      currentNode match {
        // DDL Command Checks (examples, exact classes might vary by Spark version)
        // Common DDL commands often have specific case classes or traits.
        // Note: Spark's `parsePlan` might not parse all DDLs into distinct LogicalPlan nodes without special parser modes.
        // However, some operations that modify schema or data directly can be caught.
        case _: CreateTableStatement | _: CreateTableAsSelectStatement | _: DropTableCommand | _: AlterTableCommand | _: TruncateTableCommand =>
          validationErrors = s"DDL statements (CREATE, ALTER, DROP, TRUNCATE TABLE) are not allowed in transformation SQL. Found: ${currentNode.nodeName}" :: validationErrors
        case _: CreateViewCommand | _: DropViewCommand =>
          validationErrors = s"View DDL statements (CREATE/DROP VIEW) are not allowed. Found: ${currentNode.nodeName}" :: validationErrors
        case _: CreateDatabaseCommand | _: DropDatabaseCommand | _: AlterDatabaseCommand =>
          validationErrors = s"Database DDL statements are not allowed. Found: ${currentNode.nodeName}" :: validationErrors

        // DML Command Checks (examples)
        case _: InsertIntoStatement | _: InsertIntoDir | _: InsertOverwriteTable | _: InsertOverwriteDir =>
          // InsertIntoStatement is a common one from `INSERT INTO tbl SELECT ...`
          // We might allow `InsertIntoStatement` if the child is a query, but for now, let's be strict for generic SQL_LOGIC field.
          // The design might need to differentiate SQL for full query replacement vs. SQL for CTEs.
          // For now, if it's an insert, let's flag it as potentially problematic for a generic 'transform_sql' field.
          validationErrors = s"DML statements (INSERT INTO/OVERWRITE) are not allowed directly in this SQL transformation logic. Found: ${currentNode.nodeName}. The framework handles the final insert." :: validationErrors
        case _: DeleteFromTable | _: UpdateTable => // These are less common from parsePlan for typical queries but good to check
          validationErrors = s"DML statements (DELETE, UPDATE) are not allowed. Found: ${currentNode.nodeName}" :: validationErrors

        // Placeholder for other specific unsupported logical operators if identified
        // case _: SomeUnsupportedSparkLogicalOperator =>
        //  validationErrors = "Unsupported logical operator: SomeUnsupportedSparkLogicalOperator" :: validationErrors

        // Check for specific expressions if needed (more complex)
        // currentNode.expressions.foreach { expr => expr match { ... } }\n
        case _ => // Node is acceptable or will be checked by its children
      }
      // Recursively check children
      currentNode.children.foreach(checkNode)
    }

    checkNode(plan)

    if (validationErrors.isEmpty) {
      logger.info("LogicalPlan validation successful.")
      Right(())
    } else {
      logger.warn(s"LogicalPlan validation failed with errors: ${validationErrors.mkString("; ")}")
      Left(validationErrors.reverse) // Reverse to maintain order of discovery
    }
  }
}
