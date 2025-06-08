package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.sql.{SqlParserService, LogicalPlanValidator}
import com.example.etl.logic.Scd2LogicUtil
import com.example.etl.reconciliation.ReconciliationService
import com.example.etl.dq.{DataQualityService, DataQualityReport} // Import DQ types
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.time.Instant
import scala.util.parsing.json.JSON

object Scd2LogicHandler {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long], Option[DataQualityReport]) = { // Updated signature
    logger.info(s"Executing SCD Type 2 ingestion for job_name: ${jobConfig.job_name}")

    val scdProcessingTimestamp = Timestamp.from(Instant.now())
    val scdHighDateTimestamp = Timestamp.valueOf("9999-12-31 23:59:59")

    var initialSourceRecordsCount: Option[Long] = None
    var transformedSourceRecordsCount: Option[Long] = None
    var targetCurrentRecordsCount: Option[Long] = None
    var recordsWrittenCount: Option[Long] = None
    var dataQualityReportOpt: Option[DataQualityReport] = None // For DQ results

    val scdCols = Scd2LogicUtil.ScdColumnNames(
      skIdCol = jobConfig.scd2_surrogate_key_column.getOrElse("sk_id"),
      validFromCol = jobConfig.scd2_valid_from_column.getOrElse("valid_from_ts"),
      validToCol = jobConfig.scd2_valid_to_column.getOrElse("valid_to_ts"),
      versionCol = jobConfig.scd2_version_column.getOrElse("version"),
      isCurrentCol = jobConfig.scd2_current_flag_column.getOrElse("is_current"),
      changeTypeCol = "change_type"
    )

    if (jobConfig.source_connection_details.isEmpty) throw new IllegalArgumentException(s"Source connection details required for ${jobConfig.job_name}")
    val rawSourceDF: DataFrame = jobConfig.source_type.toUpperCase match {
      case "FILE" => val p=jobConfig.source_connection_details.get;val f=jobConfig.source_format.getOrElse(throw new IllegalArgumentException("src_fmt req for FILE")).toLowerCase;val r=spark.read.format(f);jobConfig.source_format_options.foreach(o=>JSON.parseFull(o)match{case Some(m:Map[String,String])=>r.options(m)case _=>});jobConfig.source_schema.foreach(s=>r.schema(s));logger.info(s"Reading FILE src $p for ${jobConfig.job_name}");r.load(p)
      case "HIVE_TABLE" => val t=jobConfig.source_connection_details.get;logger.info(s"Reading HIVE src $t for ${jobConfig.job_name}");try spark.table(t)catch{case e:Exception=>throw new RuntimeException(s"Fail read HIVE $t: ${e.getMessage}",e)}
      case o => throw new UnsupportedOperationException(s"Source type '$o' not supported for SCD2.")
    }
    if(jobConfig.audit_level.toUpperCase!="NONE")try initialSourceRecordsCount=Some(rawSourceDF.count())catch{case e:Exception=>logger.warn(s"No count raw_src ${jobConfig.job_name}:${e.getMessage}",e)}\n    logger.info(s"Raw src read ${jobConfig.job_name}. Recs:${initialSourceRecordsCount.getOrElse("N/A")}")

    val transformedSourceDF: DataFrame = jobConfig.sql_logic match {
      case Some(s) if s.trim.nonEmpty => logger.info(s"Applying sql_logic for SCD src ${jobConfig.job_name}. SQL:${s.take(100)}"); val v=jobConfig.job_name+"_raw_scd_src";rawSourceDF.createOrReplaceTempView(v);logger.info(s"Reg raw_src view '$v' for ${jobConfig.job_name}"); val pL=SqlParserService.parse(s,spark).getOrElse(throw new RuntimeException("SQL parse fail"));LogicalPlanValidator.validate(pL) match { case Right(_) => /* Do nothing */ case Left(errs) => throw new RuntimeException(s"Plan invalid for ${jobConfig.job_name}: ${errs.mkString(";")}") };try Dataset.ofRows(spark,pL)catch{case e:Exception=>throw new RuntimeException(s"Err exec SCD SQL ${jobConfig.job_name}:${e.getMessage}",e)}\n      case _ => logger.info(s"No sql_logic, using raw_src for ${jobConfig.job_name}.");rawSourceDF
    }
    if(jobConfig.audit_level.toUpperCase!="NONE")try transformedSourceRecordsCount=Some(transformedSourceDF.count())catch{case e:Exception=>logger.warn(s"No count tfm_src ${jobConfig.job_name}:${e.getMessage}",e)}\n    logger.info(s"Tfm src for SCD2 ${jobConfig.job_name}. Recs:${transformedSourceRecordsCount.getOrElse("N/A")}");if(logger.isDebugEnabled)transformedSourceDF.printSchema()

    val targetId = jobConfig.target_table_or_path
    val targetCurrentDF: DataFrame = try {
      val tFull = jobConfig.target_type.toUpperCase match {case "FILE"|"HDFS"=>spark.read.format(jobConfig.target_format.getOrElse("parquet")).load(targetId) case "HIVE_TABLE"=>spark.table(targetId) case o=>throw new UnsupportedOperationException(s"Tgt type '$o' not supported for SCD2 read ${jobConfig.job_name}.")}\n      tFull.filter(col(scdCols.isCurrentCol)===true)
    } catch {
      case e:org.apache.spark.sql.AnalysisException if e.getMessage.toLowerCase.contains("path does not exist")||e.getMessage.toLowerCase.contains("table or view not found")=>logger.warn(s"Tgt $targetId not found for ${jobConfig.job_name}. Initial load.");val scdSF=Seq(StructField(scdCols.skIdCol,LongType,false),StructField(scdCols.validFromCol,TimestampType,false),StructField(scdCols.validToCol,TimestampType,false),StructField(scdCols.versionCol,IntegerType,false),StructField(scdCols.isCurrentCol,BooleanType,false),StructField(scdCols.changeTypeCol,StringType,true));val emptySchema=StructType(transformedSourceDF.schema.fields++scdSF);spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],emptySchema)\n      case e:Exception=>throw new RuntimeException(s"Err read tgt ${jobConfig.job_name} from $targetId:${e.getMessage}",e)}\n    if(jobConfig.audit_level.toUpperCase!="NONE")try targetCurrentRecordsCount=Some(targetCurrentDF.count())catch{case e:Exception=>logger.warn(s"No count tgt_cur ${jobConfig.job_name}:${e.getMessage}",e)}\n    logger.info(s"Cur tgt read ${jobConfig.job_name}. Active:${targetCurrentRecordsCount.getOrElse("N/A")}")

    val nkCols=jobConfig.scd2_natural_keys.getOrElse(throw new IllegalArgumentException(s"scd2_nk req for ${jobConfig.job_name}")).split(',').map(_.trim).filter(_.nonEmpty);if(nkCols.isEmpty)throw new IllegalArgumentException(s"scd2_nk empty ${jobConfig.job_name}")
    val srcBusHashCols=transformedSourceDF.columns.filterNot(nkCols.contains).toSeq
    val handleDel=jobConfig.job_properties.flatMap(JSON.parseFull(_).collect{case m:Map[String,String]=>m.get("scd2_handle_deletes_by_absence")}.flatten.map(_.toBoolean)).getOrElse(false)
    val catRecs=Scd2LogicUtil.categorizeAndDetectChanges(transformedSourceDF,targetCurrentDF,nkCols,jobConfig.scd2_change_tracking_column,srcBusHashCols,handleDel,spark)
    val tgtScdMetaCols=Seq(scdCols.skIdCol,scdCols.validFromCol,scdCols.validToCol,scdCols.versionCol,scdCols.isCurrentCol,scdCols.changeTypeCol).map(_.toLowerCase)
    val tgtBusCols=targetCurrentDF.columns.filterNot(tgtScdMetaCols.contains).toSeq
    val finalOutDF=Scd2LogicUtil.generateScdOutputRecords(catRecs,transformedSourceDF.columns.toSeq,tgtBusCols,nkCols,scdCols,scdProcessingTimestamp,scdHighDateTimestamp,spark)
    logger.info(s"Final SCD output gen for ${jobConfig.job_name}.")

    val writer=finalOutDF.write.mode(SaveMode.Append);jobConfig.target_format.foreach(f=>writer.format(f.toLowerCase));jobConfig.target_format_options.foreach(o=>JSON.parseFull(o)match{case Some(m:Map[String,String])=>writer.options(m)case _=>});jobConfig.partitioning_columns.foreach(pc=>{val c=pc.split(',').map(_.trim).filter(_.nonEmpty);if(c.nonEmpty)writer.partitionBy(c:_*)})\n    jobConfig.target_type.toUpperCase match {case "HDFS"|"FILE"=>writer.save(targetId) case "HIVE_TABLE"=>logger.info(s"Saving to Hive $targetId for ${jobConfig.job_name}");spark.sqlContext.setConf("hive.exec.dynamic.partition","true");spark.sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict");writer.insertInto(targetId) case o=>throw new UnsupportedOperationException(s"Tgt type '$o' not supported SCD2 ${jobConfig.job_name}.")}\n    logger.info(s"SCD recs written for ${jobConfig.job_name}.")
    if(jobConfig.audit_level.toUpperCase!="NONE")try recordsWrittenCount=Some(finalOutDF.count())catch{case e:Exception=>logger.warn(s"No count final_out ${jobConfig.job_name}:${e.getMessage}",e)}\n\n    if(jobConfig.reconciliation_enabled.equalsIgnoreCase("Y")&&jobConfig.reconciliation_config.exists(_.trim.nonEmpty)){logger.info(s"Recon for ${jobConfig.job_name}(SCD2 Current View)..." );try{val sRecon=transformedSourceDF;val curTgtSnap=jobConfig.target_type.toUpperCase match {case "HDFS"|"FILE"=>spark.read.format(jobConfig.target_format.getOrElse("parquet")).load(targetId).filter(col(scdCols.isCurrentCol)===true) case "HIVE_TABLE"=>spark.table(targetId).filter(col(scdCols.isCurrentCol)===true) case o=>throw new RuntimeException(s"Cannot get tgt snap for SCD2 recon type $o ${jobConfig.job_name}")};val rep=ReconciliationService.executeChecks(jobConfig.job_name,sRecon,curTgtSnap,jobConfig.reconciliation_config.get,spark);logger.info(s"Recon Report ${jobConfig.job_name}(SCD2):Status-${rep.overallStatus}");rep.checkResults.foreach(cr=>logger.info(s" Ck:${cr.checkType},St:${cr.status},Msg:${cr.message}"));if(rep.overallStatus!="PASS")logger.warn(s"Recon ${jobConfig.job_name}(SCD2) status:${rep.overallStatus}.")}catch{case e:Exception=>logger.error(s"Err SCD2 recon ${jobConfig.job_name}:${e.getMessage}",e)}}\n\n    // --- Data Quality Checks --- \n    if (jobConfig.dq_checks_enabled.equalsIgnoreCase("Y") && jobConfig.dq_rules_config.exists(_.trim.nonEmpty)) {\n      logger.info(s"Starting DQ checks for job ${jobConfig.job_name} on current target view...")\n      try {\n        val targetCurrentSnapshotDF = jobConfig.target_type.toUpperCase match {\n            case "HDFS" | "FILE" => spark.read.format(jobConfig.target_format.getOrElse("parquet")).load(targetId).filter(col(scdCols.isCurrentCol) === true)\n            case "HIVE_TABLE" => spark.table(targetId).filter(col(scdCols.isCurrentCol) === true)\n            case other => throw new RuntimeException("Cannot get target snapshot for DQ for unsupported target type " + other + s" for job ${jobConfig.job_name}")\n        }\n        val dqReport = DataQualityService.applyChecks(jobConfig.job_name, targetCurrentSnapshotDF, jobConfig.dq_rules_config.get, spark)\n        dataQualityReportOpt = Some(dqReport)\n        logger.info(s"DQ Report for ${jobConfig.job_name}: Status - ${dqReport.overallStatus}")\n        dqReport.rule_results.foreach(rr => logger.info(s"  DQ Rule: ${rr.rule_name}, Status: ${rr.status}, Failing: ${rr.failing_records_count.getOrElse("N/A")}, Msg: ${rr.message}"))\n        if (dqReport.overallStatus == "FAIL") {\n          logger.error(s"Critical DQ checks failed for job ${jobConfig.job_name}. Status: FAIL. Failing job.")\n          throw new RuntimeException(s"Critical DQ checks failed for job ${jobConfig.job_name}. Status: FAIL.")\n        } else if (dqReport.overallStatus == "WARN") {\n           logger.warn(s"DQ checks passed with warnings for job ${jobConfig.job_name}. Status: WARN.")\n        }\n      } catch {\n        case e: RuntimeException if e.getMessage.startsWith("Critical DQ checks failed") => throw e \n        case e: Exception => \n          logger.error(s"Error during DQ process for job ${jobConfig.job_name}: ${e.getMessage}", e)\n          throw new RuntimeException(s"Error in DQ subsystem for job ${jobConfig.job_name}", e) \n      }\n    }\n\n    logger.info(s"SCD2 ingestion for ${jobConfig.job_name} completed. Records written/affected: ${recordsWrittenCount.getOrElse("N/A")}")\n    (transformedSourceRecordsCount, recordsWrittenCount, dataQualityReportOpt) // Updated return tuple\n  }\n}
