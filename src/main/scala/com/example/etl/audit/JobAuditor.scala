package com.example.etl.audit

import com.example.etl.config.JobConfig
import org.apache.log4j.Logger
import java.io.{FileWriter, BufferedWriter, PrintWriter}
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class AuditRecord(
  timestamp: String,
  jobId: String,
  jobName: String,
  status: String,
  recordsRead: Option[Long] = None,
  recordsWritten: Option[Long] = None,
  message: Option[String] = None
) {
  def toCsv: String = {
    def escape(s: String): String = s.replace("\"", "\"\"")
    val ts = escape(timestamp)
    val jId = escape(jobId)
    val jName = escape(jobName)
    val stat = escape(status)
    val rr = recordsRead.map(_.toString).getOrElse("")
    val rw = recordsWritten.map(_.toString).getOrElse("")
    val msg = message.map(escape).getOrElse("")
    s"\"${ts}\",\"${jId}\",\"${jName}\",\"${stat}\",\"${rr}\",\"${rw}\",\"${msg}\""
  }
}

case class RejectionRecord(
  timestamp: String,
  jobId: String,
  jobName: String,
  stage: String,
  recordIdentifier: Option[String] = None,
  reason: String,
  rejectedData: Option[String] = None
) {
  def toCsv: String = {
    def escape(s: String): String = s.replace("\"", "\"\"")
    val ts = escape(timestamp)
    val jId = escape(jobId)
    val jName = escape(jobName)
    val stg = escape(stage)
    val ri = recordIdentifier.map(escape).getOrElse("")
    val rsn = escape(reason)
    val rd = rejectedData.map(escape).getOrElse("")
    s"\"${ts}\",\"${jId}\",\"${jName}\",\"${stg}\",\"${ri}\",\"${rsn}\",\"${rd}\""
  }
}

object JobAuditor {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  private val auditDateTimeFormat = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  private var auditFilePathInternal: Option[String] = None
  private val RejectionFileSuffix = "_rejections"

  def init(filePath: String): Unit = {
    auditFilePathInternal = Some(filePath)
    val file = Paths.get(filePath).toFile
    if (!file.exists() || file.length() == 0) {
      try {
        if (file.getParentFile != null) {
            file.getParentFile.mkdirs()
        }
        val writer = new PrintWriter(new BufferedWriter(new FileWriter(file, false)))
        try {
          writer.println("\"timestamp\",\"jobId\",\"jobName\",\"status\",\"recordsRead\",\"recordsWritten\",\"message\"")
        } finally {
          writer.close()
        }
      } catch {
        case e: Exception => logger.error(s"Failed to write audit header to $filePath", e)
      }
    }
    logger.info(s"JobAuditor initialized. Audit logs will be written to: $filePath")
  }

  private def getCurrentTimestamp: String = LocalDateTime.now().format(auditDateTimeFormat)

  private def writeToAuditFile(filePath: String, header: String, contentCsv: String, isHeaderPresentCheck: () => Boolean, setHeaderPresentFlag: () => Unit): Unit = {
    try {
      val file = Paths.get(filePath).toFile
      if (file.getParentFile != null && !file.getParentFile.exists()) {
          file.getParentFile.mkdirs()
      }
      val needsHeader = !file.exists() || file.length() == 0 // Simplified header check based on file existence/size
      val writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true))) // Append mode
      try {
        if (needsHeader) {
          writer.println(header)
        }
        writer.println(contentCsv)
      } finally {
        writer.close()
      }
    } catch {
      case e: Exception => logger.error(s"Failed to write to file $filePath: ${e.getMessage}", e)
    }
  }

  private def commitAuditRecord(record: AuditRecord): Unit = {
    auditFilePathInternal match {
      case Some(path) =>
        val header = "\"timestamp\",\"jobId\",\"jobName\",\"status\",\"recordsRead\",\"recordsWritten\",\"message\""
        // The main audit file header is written by init, so no need for complex header check here for append
        writeToAuditFile(path, header, record.toCsv, () => Files.exists(Paths.get(path)) && Files.size(Paths.get(path)) > 0, () => {})
      case None =>
        logger.warn(s"Audit file path not initialized. Audit record not written to file: ${record.toCsv}")
    }
    logger.info(s"Audit Event: ${record.status} - JobId: ${record.jobId}, Details: ${record.toCsv}")
  }

  def logJobStart(jobConfig: JobConfig): Unit = {
    val record = AuditRecord(getCurrentTimestamp, jobConfig.job_id, jobConfig.job_name, "STARTED")
    commitAuditRecord(record)
  }

  def logJobSuccess(jobConfig: JobConfig, recordsRead: Option[Long] = None, recordsWritten: Option[Long] = None, message: Option[String] = None): Unit = {
    val record = AuditRecord(getCurrentTimestamp, jobConfig.job_id, jobConfig.job_name, "SUCCESS", recordsRead, recordsWritten, message)
    commitAuditRecord(record)
  }

  def logJobFailure(jobConfig: JobConfig, error: Throwable, recordsRead: Option[Long] = None, recordsWritten: Option[Long] = None): Unit = {
    val errorMessage = s"${error.getClass.getName}: ${error.getMessage}"
    val record = AuditRecord(getCurrentTimestamp, jobConfig.job_id, jobConfig.job_name, "FAILED", recordsRead, recordsWritten, Some(errorMessage))
    commitAuditRecord(record)
  }

  // New method for logging rejections
  def logRejection(
    jobConfig: JobConfig,
    stage: String,
    reason: String,
    recordIdentifier: Option[String] = None,
    rejectedData: Option[String] = None
  ): Unit = {
    val record = RejectionRecord(
      getCurrentTimestamp,
      jobConfig.job_id,
      jobConfig.job_name,
      stage,
      recordIdentifier,
      reason,
      rejectedData
    )
    auditFilePathInternal match {
      case Some(mainAuditPath) =>
        val parts = mainAuditPath.split("[/\\\\]") // Split by slash or backslash
        val fileName = parts.last
        val dirPath = if (parts.length > 1) parts.dropRight(1).mkString(java.io.File.separator) else "." // Handle if no dir path

        val rejectionFileName = fileName.lastIndexOf('.') match {
            case -1 => fileName + RejectionFileSuffix // No extension
            case dotIdx => fileName.substring(0, dotIdx) + RejectionFileSuffix + fileName.substring(dotIdx)
        }
        val rejectionFilePath = Paths.get(dirPath, rejectionFileName).toString

        val header = "\"timestamp\",\"jobId\",\"jobName\",\"stage\",\"recordIdentifier\",\"reason\",\"rejectedData\""
        // Use file existence/size for header check directly in writeToAuditFile
        writeToAuditFile(rejectionFilePath, header, record.toCsv,
          () => true, // This is a placeholder, actual check is inside writeToAuditFile
          () => {}
        )
        logger.info(s"Rejection Logged for JobId ${record.jobId}, Stage: ${record.stage}, Reason: ${record.reason}. Data written to $rejectionFilePath")
      case None =>
        logger.warn(s"Audit file path not initialized. Rejection record not written to file (logged to console only): ${record.toCsv}")
        logger.info(s"Rejection Event (console only): ${record.stage} - JobId ${record.jobId}, Reason: ${record.reason}")
    }
  }
}
