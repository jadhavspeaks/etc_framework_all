package com.example.etl.notification

import com.example.etl.config.JobConfig
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scala.io.Source
import scala.util.{Try, Failure, Success, Using}
import jakarta.mail.{Authenticator, Message, PasswordAuthentication, Session, Transport}
import jakarta.mail.internet.{InternetAddress, MimeMessage}
import java.util.Properties

object NotificationService {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  // SMTP Configuration keys (expected in Spark conf)
  val SmtpHostKey = "spark.etl.smtp.host"
  val SmtpPortKey = "spark.etl.smtp.port"
  val SmtpUserKey = "spark.etl.smtp.user"
  val SmtpPasswordKey = "spark.etl.smtp.password"
  val SmtpAuthKey = "spark.etl.smtp.auth" // true/false
  val SmtpStartTlsKey = "spark.etl.smtp.starttls.enable" // true/false
  val SmtpFromKey = "spark.etl.smtp.from"
  val SmtpSenderNameKey = "spark.etl.smtp.sendername" // Optional display name for sender

  private def loadTemplate(templateName: String): Try[String] = Try {
    val resourceStream = getClass.getResourceAsStream(s"/templates/$templateName")
    if (resourceStream == null) throw new java.io.FileNotFoundException(s"Template $templateName not found in resources/templates")
    Using(Source.fromInputStream(resourceStream)("UTF-8"))(_.mkString).get
  }

  private def populateTemplate(templateContent: String, metrics: Map[String, String]): String = {
    metrics.foldLeft(templateContent) { case (content, (placeholder, value)) =>
      content.replace(s"{{${placeholder.toUpperCase}}}", value)
    }
  }

  def sendNotification(
    jobConfig: JobConfig,
    status: String, // e.g., "SUCCESS", "FAILED", "SKIPPED_INACTIVE"
    metrics: Map[String, String], // Key-value pairs for template placeholders
    spark: SparkSession
  ): Unit = {
    logger.info(s"Attempting to send ${status.toLowerCase} notification for job: ${jobConfig.job_name}")

    val templateName = status.toLowerCase match {
      case "success" => "success_email_template.html"
      case "failed" => "failure_email_template.html"
      case "skipped_inactive" => "inactive_email_template.html"
      case _ =>
        logger.warn(s"Unknown notification status '$status' for job ${jobConfig.job_name}. No template defined.")
        return // Exit if no template matches
    }

    val recipients: Option[String] = status.toLowerCase match {
        case "success" => jobConfig.notification_emails_success
        case "failed" | "skipped_inactive" => jobConfig.notification_emails_failure // Also notify failure list for skipped/inactive
        case _ => None
    }

    if (recipients.isEmpty || recipients.get.trim.isEmpty) {
      logger.info(s"No recipients configured for ${status.toLowerCase} notifications for job ${jobConfig.job_name}. Skipping email.")
      return
    }

    loadTemplate(templateName) match {
      case Failure(e) =>
        logger.error(s"Failed to load email template '$templateName' for job ${jobConfig.job_name}: ${e.getMessage}", e)
      case Success(templateContent) =>
        val mailSubject = s"ETL Job ${metrics.getOrElse("JOB_NAME", jobConfig.job_name)} - $status"\n        val mailBody = populateTemplate(templateContent, metrics ++ Map("JOB_NAME" -> jobConfig.job_name, "STATUS" -> status))\n        \n        Try {\n          val smtpHost = spark.conf.get(SmtpHostKey)\n          val smtpPort = spark.conf.get(SmtpPortKey, "587") // Default to 587 for TLS, 25 for non-TLS\n          val smtpUser = spark.conf.get(SmtpUserKey, "")\n          val smtpPassword = spark.conf.get(SmtpPasswordKey, "")\n          val smtpAuth = spark.conf.get(SmtpAuthKey, "true").toBoolean\n          val smtpStartTls = spark.conf.get(SmtpStartTlsKey, "true").toBoolean\n          val smtpFrom = spark.conf.get(SmtpFromKey)\n          val senderDisplayName = spark.conf.get(SmtpSenderNameKey, "ETL Framework")\n\n          if (smtpHost == null || smtpFrom == null) {\n            throw new IllegalArgumentException(s"Missing required SMTP configuration: $SmtpHostKey or $SmtpFromKey")\n          }\n\n          val props = new Properties()\n          props.put("mail.smtp.host", smtpHost)\n          props.put("mail.smtp.port", smtpPort)\n          if (smtpAuth) props.put("mail.smtp.auth", "true")\n          if (smtpStartTls) props.put("mail.smtp.starttls.enable", "true")\n          // Add more properties if needed, e.g., for SSL: mail.smtp.socketFactory.port, mail.smtp.socketFactory.class (javax.net.ssl.SSLSocketFactory)\n\n          val session = if (smtpAuth && smtpUser.nonEmpty) {\n            Session.getInstance(props, new Authenticator() {\n              override def getPasswordAuthentication = new PasswordAuthentication(smtpUser, smtpPassword)\n            })\n          } else {\n            Session.getInstance(props)\n          }\n\n          val message = new MimeMessage(session)\n          val fromAddress = new InternetAddress(smtpFrom, senderDisplayName)\n          message.setFrom(fromAddress)\n          recipients.get.split(",").map(_.trim).filter(_.nonEmpty).foreach { recipient =>\n            message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient))\n          }\n          message.setSubject(mailSubject)\n          message.setContent(mailBody, "text/html; charset=utf-8") // Corrected charset\n          message.setSentDate(new java.util.Date())\n\n          Transport.send(message)\n          logger.info(s"Notification email sent successfully for job ${jobConfig.job_name} to: ${recipients.get}")\n        } match {\n          case Failure(e) => \n            logger.error(s"Failed to send notification email for job ${jobConfig.job_name}: ${e.getMessage}", e)\n        }\n    }\n  }\n}
