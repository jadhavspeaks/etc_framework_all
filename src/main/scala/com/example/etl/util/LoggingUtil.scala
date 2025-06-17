package com.example.etl.util

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.log4j.Logger

object LoggingUtil {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   * Masks specified columns in a DataFrame for logging purposes.
   * Replaces non-null values in the specified columns with a mask string.
   *
   * @param df The input DataFrame.
   * @param columnsToMask A sequence of column names to be masked.
   * @param maskValue The string to use as a mask (default is "******").
   * @return A new DataFrame with the specified columns masked.
   */
  def maskDataFrame(df: DataFrame, columnsToMask: Seq[String], maskValue: String = "******"): DataFrame = {
    if (columnsToMask.isEmpty) {
      return df
    }

    var tempDf = df
    val dfColumnsLower = df.columns.map(_.toLowerCase).toSet

    columnsToMask.foreach { colName =>
      if (dfColumnsLower.contains(colName.toLowerCase)) {
        // Find the actual column name to preserve case if possible, though Spark is case-insensitive for resolution here
        val actualColName = df.columns.find(_.equalsIgnoreCase(colName)).getOrElse(colName)
        logger.debug(s"Masking column: $actualColName")
        tempDf = tempDf.withColumn(actualColName,
          when(col(actualColName).isNotNull, lit(maskValue)).otherwise(lit(null))
        )
      } else {
        logger.warn(s"Column '$colName' specified for masking not found in DataFrame. Available columns: ${df.columns.mkString(", ")}")
      }
    }
    tempDf
  }
}
