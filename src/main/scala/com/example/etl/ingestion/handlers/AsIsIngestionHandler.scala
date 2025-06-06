package com.example.etl.ingestion.handlers

import com.example.etl.config.JobConfig
import com.example.etl.util.SchemaMapper
import com.example.etl.reconciliation.ReconciliationService
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.log4j.Logger
import scala.util.parsing.json.JSON

object AsIsIngestionHandler {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long]) = {
    logger.info(s"Executing AS_IS ingestion for job_name: ${jobConfig.job_name}")
    var rReadCount: Option[Long] = None; var rWrittenCount: Option[Long] = None
    if (jobConfig.source_connection_details.isEmpty) throw new IllegalArgumentException(s"Source connection details required for job ${jobConfig.job_name}")
    var srcDF: DataFrame = null
    jobConfig.source_type.toUpperCase match {
      case "FILE" => val fmt = jobConfig.source_format.getOrElse(throw new IllegalArgumentException("source_format required for FILE type")).toLowerCase; val rdr = spark.read.format(fmt); jobConfig.source_format_options.foreach(o => JSON.parseFull(o) match {case Some(m:Map[String,String])=>rdr.options(m) case _=>logger.warn(s"Could not parse source_format_options: $o")}); jobConfig.source_schema.foreach(s=>rdr.schema(s)); srcDF = rdr.load(jobConfig.source_connection_details.get)
      case o => throw new UnsupportedOperationException(s"Source type '$o' not supported.")
    }
    if(srcDF==null) throw new RuntimeException("Source DF not loaded.")
    if(jobConfig.audit_level.toUpperCase!="NONE") try rReadCount=Some(srcDF.count()) catch {case e:Exception=>logger.warn(s"Could not count source records for ${jobConfig.job_name}: ${e.getMessage}",e)}\n    logger.info(s"Source data read for ${jobConfig.job_name}. Records: ${rReadCount.getOrElse("N/A")}")
    var tDF=srcDF; jobConfig.schema_mapping_logic match {case Some(mj) if mj.trim.nonEmpty => val mi=SchemaMapper.parseMappingLogic(mj); if(mi.nonEmpty){ val b=jobConfig.job_properties.flatMap(p=>JSON.parseFull(p) match {case Some(m:Map[String,String])=>m.get("schema_drift_new_source_columns_behavior") case _=>None}).getOrElse("ignore"); tDF=SchemaMapper.applyMapping(srcDF,mi,spark,b)} else logger.warn(s"No mapping instructions from schema_mapping_logic for ${jobConfig.job_name}.")} case _=>logger.info(s"No schema_mapping_logic for ${jobConfig.job_name}.")}\n    val wr=tDF.write; jobConfig.target_format.foreach(f=>wr.format(f.toLowerCase)); jobConfig.target_format_options.foreach(o=>JSON.parseFull(o) match {case Some(m:Map[String,String])=>wr.options(m) case _=>}); jobConfig.partitioning_columns.foreach(pc=>{val c=pc.split(',').map(_.trim).filter(_.nonEmpty); if(c.nonEmpty)wr.partitionBy(c:_*) }); val sm=jobConfig.target_write_mode.toLowerCase match {case "overwrite"=>SaveMode.Overwrite case "append"=>SaveMode.Append case "ignore"=>SaveMode.Ignore case "errorifexists"|"error"=>SaveMode.ErrorIfExists case _=>logger.warn(s"Unsupported save mode '${jobConfig.target_write_mode}'. Defaulting to Overwrite.");SaveMode.Overwrite}; wr.mode(sm)
    jobConfig.target_type.toUpperCase match {case "HDFS"|"FILE" => wr.save(jobConfig.target_table_or_path); if(jobConfig.audit_level.toUpperCase!="NONE") try {val tFmt=jobConfig.target_format.getOrElse(jobConfig.source_format.getOrElse("parquet")); rWrittenCount=Some(spark.read.format(tFmt).load(jobConfig.target_table_or_path).count())} catch {case e:Exception=>logger.warn(s"Could not count written records for ${jobConfig.job_name}: ${e.getMessage}",e)} case o => throw new UnsupportedOperationException(s"Target type '$o' not supported.")}\n    if(jobConfig.reconciliation_enabled.equalsIgnoreCase("Y")&&jobConfig.reconciliation_config.exists(_.trim.nonEmpty)){logger.info(s"Reconciliation for ${jobConfig.job_name}..."); try {val sRecon=tDF;val tFmt=jobConfig.target_format.getOrElse("parquet"); val tSnap=spark.read.format(tFmt).load(jobConfig.target_table_or_path); val rep=ReconciliationService.executeChecks(jobConfig.job_name,sRecon,tSnap,jobConfig.reconciliation_config.get,spark); logger.info(s"Recon Report for ${jobConfig.job_name}: Status - ${rep.overallStatus}"); rep.checkResults.foreach(cr=>logger.info(s"  Check:${cr.checkType},Status:${cr.status},Src:${cr.sourceValue},Tgt:${cr.targetValue},Diff:${cr.difference.getOrElse("N/A")},Msg:${cr.message}")); if(rep.overallStatus!="PASS")logger.warn(s"Recon for ${jobConfig.job_name} status: ${rep.overallStatus}.")} catch {case e:Exception=>logger.error(s"Error during recon for ${jobConfig.job_name}: ${e.getMessage}",e)}}\n    logger.info(s"AS_IS ingestion for job_name: ${jobConfig.job_name} completed."); (rReadCount,rWrittenCount)}\n}
