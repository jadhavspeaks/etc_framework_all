package com.example.etl.api.entity;

import javax.persistence.*;
import java.sql.Timestamp;
// Consider using org.hibernate.annotations.Type for CLOB if default @Lob is problematic with Oracle specific LOB types or for better control

@Entity
@Table(name = "JOB_CONFIG")
public class JobConfigEntity {

    @Id
    @Column(name = "job_name", length = 255, nullable = false)
    private String jobName;

    @Column(name = "job_description", length = 1000)
    private String jobDescription;

    @Column(name = "is_active", length = 1, nullable = false)
    private String isActive; // Y or N

    @Column(name = "source_type", length = 50, nullable = false)
    private String sourceType;

    @Column(name = "source_connection_details", length = 4000)
    private String sourceConnectionDetails;

    @Column(name = "source_format", length = 50)
    private String sourceFormat;

    @Column(name = "source_format_options", length = 1000)
    private String sourceFormatOptions; // JSON String

    @Lob
    @Column(name = "source_schema")
    private String sourceSchema; // CLOB

    @Column(name = "target_type", length = 50, nullable = false)
    private String targetType;

    @Column(name = "target_connection_details", length = 4000)
    private String targetConnectionDetails;

    @Column(name = "target_format", length = 50)
    private String targetFormat;

    @Column(name = "target_format_options", length = 1000)
    private String targetFormatOptions; // JSON String

    @Column(name = "target_table_or_path", length = 1000, nullable = false)
    private String targetTableOrPath;

    @Column(name = "load_type", length = 50, nullable = false)
    private String loadType;

    @Column(name = "target_write_mode", length = 50, nullable = false)
    private String targetWriteMode;

    @Column(name = "transformation_mode", length = 50, nullable = false)
    private String transformationMode;

    @Lob
    @Column(name = "sql_logic")
    private String sqlLogic; // CLOB

    @Lob
    @Column(name = "schema_mapping_logic")
    private String schemaMappingLogic; // CLOB (JSON String)

    @Column(name = "scd2_natural_keys", length = 1000)
    private String scd2NaturalKeys;

    @Column(name = "scd2_change_tracking_column", length = 100)
    private String scd2ChangeTrackingColumn;

    @Column(name = "scd2_surrogate_key_column", length = 100)
    private String scd2SurrogateKeyColumn;

    @Column(name = "scd2_valid_from_column", length = 100)
    private String scd2ValidFromColumn;

    @Column(name = "scd2_valid_to_column", length = 100)
    private String scd2ValidToColumn;

    @Column(name = "scd2_version_column", length = 100)
    private String scd2VersionColumn;

    @Column(name = "scd2_current_flag_column", length = 100)
    private String scd2CurrentFlagColumn;

    @Column(name = "partitioning_columns", length = 1000)
    private String partitioningColumns;

    @Column(name = "job_priority")
    private Integer jobPriority; // Oracle NUMBER maps to Integer/Long via BigDecimal

    @Column(name = "max_retries")
    private Integer maxRetries;

    @Column(name = "retry_backoff_ms")
    private Long retryBackoffMs;

    @Column(name = "sla_threshold_minutes")
    private Integer slaThresholdMinutes;

    @Column(name = "dependency_job_ids", length = 4000) // Stores job_names
    private String dependencyJobIds;

    @Column(name = "audit_level", length = 50, nullable = false)
    private String auditLevel;

    @Column(name = "reconciliation_enabled", length = 1, nullable = false)
    private String reconciliationEnabled; // Y or N

    @Lob
    @Column(name = "reconciliation_config")
    private String reconciliationConfig; // CLOB (JSON String)

    @Column(name = "dq_checks_enabled", length = 1, nullable = false)
    private String dqChecksEnabled; // Y or N

    @Lob
    @Column(name = "dq_rules_config")
    private String dqRulesConfig; // CLOB (JSON String)

    @Column(name = "notification_emails_success", length = 4000)
    private String notificationEmailsSuccess;

    @Column(name = "notification_emails_failure", length = 4000)
    private String notificationEmailsFailure;

    @Column(name = "notification_verbosity", length = 50)
    private String notificationVerbosity;

    @Lob
    @Column(name = "job_properties")
    private String jobProperties; // CLOB (JSON String)

    @Column(name = "log_masking_columns", length = 1000)
    private String logMaskingColumns;

    @Column(name = "execution_authorization_flag", length = 1, nullable = false)
    private String executionAuthorizationFlag; // Y or N

    @Column(name = "manual_trigger_only", length = 1, nullable = false)
    private String manualTriggerOnly; // Y or N

    @Column(name = "created_by", length = 100, nullable = false, updatable = false)
    private String createdBy;

    @Column(name = "created_ts", nullable = false, updatable = false)
    @Temporal(TemporalType.TIMESTAMP) // For java.util.Date or Calendar. For java.sql.Timestamp, this is often not needed but doesn't hurt.
    private Timestamp createdTs; // Using java.sql.Timestamp directly

    @Column(name = "updated_by", length = 100, nullable = false)
    private String updatedBy;

    @Column(name = "updated_ts", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Timestamp updatedTs;

    @Column(name = "config_version", nullable = false)
    private Integer configVersion;

    // Getters and Setters
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }

    public String getJobDescription() { return jobDescription; }
    public void setJobDescription(String jobDescription) { this.jobDescription = jobDescription; }

    public String getIsActive() { return isActive; }
    public void setIsActive(String isActive) { this.isActive = isActive; }

    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }

    public String getSourceConnectionDetails() { return sourceConnectionDetails; }
    public void setSourceConnectionDetails(String sourceConnectionDetails) { this.sourceConnectionDetails = sourceConnectionDetails; }

    public String getSourceFormat() { return sourceFormat; }
    public void setSourceFormat(String sourceFormat) { this.sourceFormat = sourceFormat; }

    public String getSourceFormatOptions() { return sourceFormatOptions; }
    public void setSourceFormatOptions(String sourceFormatOptions) { this.sourceFormatOptions = sourceFormatOptions; }

    public String getSourceSchema() { return sourceSchema; }
    public void setSourceSchema(String sourceSchema) { this.sourceSchema = sourceSchema; }

    public String getTargetType() { return targetType; }
    public void setTargetType(String targetType) { this.targetType = targetType; }

    public String getTargetConnectionDetails() { return targetConnectionDetails; }
    public void setTargetConnectionDetails(String targetConnectionDetails) { this.targetConnectionDetails = targetConnectionDetails; }

    public String getTargetFormat() { return targetFormat; }
    public void setTargetFormat(String targetFormat) { this.targetFormat = targetFormat; }

    public String getTargetFormatOptions() { return targetFormatOptions; }
    public void setTargetFormatOptions(String targetFormatOptions) { this.targetFormatOptions = targetFormatOptions; }

    public String getTargetTableOrPath() { return targetTableOrPath; }
    public void setTargetTableOrPath(String targetTableOrPath) { this.targetTableOrPath = targetTableOrPath; }

    public String getLoadType() { return loadType; }
    public void setLoadType(String loadType) { this.loadType = loadType; }

    public String getTargetWriteMode() { return targetWriteMode; }
    public void setTargetWriteMode(String targetWriteMode) { this.targetWriteMode = targetWriteMode; }

    public String getTransformationMode() { return transformationMode; }
    public void setTransformationMode(String transformationMode) { this.transformationMode = transformationMode; }

    public String getSqlLogic() { return sqlLogic; }
    public void setSqlLogic(String sqlLogic) { this.sqlLogic = sqlLogic; }

    public String getSchemaMappingLogic() { return schemaMappingLogic; }
    public void setSchemaMappingLogic(String schemaMappingLogic) { this.schemaMappingLogic = schemaMappingLogic; }

    public String getScd2NaturalKeys() { return scd2NaturalKeys; }
    public void setScd2NaturalKeys(String scd2NaturalKeys) { this.scd2NaturalKeys = scd2NaturalKeys; }

    public String getScd2ChangeTrackingColumn() { return scd2ChangeTrackingColumn; }
    public void setScd2ChangeTrackingColumn(String scd2ChangeTrackingColumn) { this.scd2ChangeTrackingColumn = scd2ChangeTrackingColumn; }

    public String getScd2SurrogateKeyColumn() { return scd2SurrogateKeyColumn; }
    public void setScd2SurrogateKeyColumn(String scd2SurrogateKeyColumn) { this.scd2SurrogateKeyColumn = scd2SurrogateKeyColumn; }

    public String getScd2ValidFromColumn() { return scd2ValidFromColumn; }
    public void setScd2ValidFromColumn(String scd2ValidFromColumn) { this.scd2ValidFromColumn = scd2ValidFromColumn; }

    public String getScd2ValidToColumn() { return scd2ValidToColumn; }
    public void setScd2ValidToColumn(String scd2ValidToColumn) { this.scd2ValidToColumn = scd2ValidToColumn; }

    public String getScd2VersionColumn() { return scd2VersionColumn; }
    public void setScd2VersionColumn(String scd2VersionColumn) { this.scd2VersionColumn = scd2VersionColumn; }

    public String getScd2CurrentFlagColumn() { return scd2CurrentFlagColumn; }
    public void setScd2CurrentFlagColumn(String scd2CurrentFlagColumn) { this.scd2CurrentFlagColumn = scd2CurrentFlagColumn; }

    public String getPartitioningColumns() { return partitioningColumns; }
    public void setPartitioningColumns(String partitioningColumns) { this.partitioningColumns = partitioningColumns; }

    public Integer getJobPriority() { return jobPriority; }
    public void setJobPriority(Integer jobPriority) { this.jobPriority = jobPriority; }

    public Integer getMaxRetries() { return maxRetries; }
    public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }

    public Long getRetryBackoffMs() { return retryBackoffMs; }
    public void setRetryBackoffMs(Long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }

    public Integer getSlaThresholdMinutes() { return slaThresholdMinutes; }
    public void setSlaThresholdMinutes(Integer slaThresholdMinutes) { this.slaThresholdMinutes = slaThresholdMinutes; }

    public String getDependencyJobIds() { return dependencyJobIds; }
    public void setDependencyJobIds(String dependencyJobIds) { this.dependencyJobIds = dependencyJobIds; }

    public String getAuditLevel() { return auditLevel; }
    public void setAuditLevel(String auditLevel) { this.auditLevel = auditLevel; }

    public String getReconciliationEnabled() { return reconciliationEnabled; }
    public void setReconciliationEnabled(String reconciliationEnabled) { this.reconciliationEnabled = reconciliationEnabled; }

    public String getReconciliationConfig() { return reconciliationConfig; }
    public void setReconciliationConfig(String reconciliationConfig) { this.reconciliationConfig = reconciliationConfig; }

    public String getDqChecksEnabled() { return dqChecksEnabled; }
    public void setDqChecksEnabled(String dqChecksEnabled) { this.dqChecksEnabled = dqChecksEnabled; }

    public String getDqRulesConfig() { return dqRulesConfig; }
    public void setDqRulesConfig(String dqRulesConfig) { this.dqRulesConfig = dqRulesConfig; }

    public String getNotificationEmailsSuccess() { return notificationEmailsSuccess; }
    public void setNotificationEmailsSuccess(String notificationEmailsSuccess) { this.notificationEmailsSuccess = notificationEmailsSuccess; }

    public String getNotificationEmailsFailure() { return notificationEmailsFailure; }
    public void setNotificationEmailsFailure(String notificationEmailsFailure) { this.notificationEmailsFailure = notificationEmailsFailure; }

    public String getNotificationVerbosity() { return notificationVerbosity; }
    public void setNotificationVerbosity(String notificationVerbosity) { this.notificationVerbosity = notificationVerbosity; }

    public String getJobProperties() { return jobProperties; }
    public void setJobProperties(String jobProperties) { this.jobProperties = jobProperties; }

    public String getLogMaskingColumns() { return logMaskingColumns; }
    public void setLogMaskingColumns(String logMaskingColumns) { this.logMaskingColumns = logMaskingColumns; }

    public String getExecutionAuthorizationFlag() { return executionAuthorizationFlag; }
    public void setExecutionAuthorizationFlag(String executionAuthorizationFlag) { this.executionAuthorizationFlag = executionAuthorizationFlag; }

    public String getManualTriggerOnly() { return manualTriggerOnly; }
    public void setManualTriggerOnly(String manualTriggerOnly) { this.manualTriggerOnly = manualTriggerOnly; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public Timestamp getCreatedTs() { return createdTs; }
    public void setCreatedTs(Timestamp createdTs) { this.createdTs = createdTs; }

    public String getUpdatedBy() { return updatedBy; }
    public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }

    public Timestamp getUpdatedTs() { return updatedTs; }
    public void setUpdatedTs(Timestamp updatedTs) { this.updatedTs = updatedTs; }

    public Integer getConfigVersion() { return configVersion; }
    public void setConfigVersion(Integer configVersion) { this.configVersion = configVersion; }
}
