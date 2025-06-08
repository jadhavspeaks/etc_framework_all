package com.example.etl.api.dto;

import java.sql.Timestamp;

public class JobConfigDetailDTO {
    private String jobName;
    private String jobDescription;
    private String isActive;
    private String sourceType;
    private String sourceConnectionDetails;
    private String sourceFormat;
    private String sourceFormatOptions; // JSON String
    private String sourceSchema; // CLOB as String
    private String targetType;
    private String targetConnectionDetails;
    private String targetFormat;
    private String targetFormatOptions; // JSON String
    private String targetTableOrPath;
    private String loadType;
    private String targetWriteMode;
    private String transformationMode;
    private String sqlLogic; // CLOB as String
    private String schemaMappingLogic; // CLOB as String
    private String scd2NaturalKeys;
    private String scd2ChangeTrackingColumn;
    private String scd2SurrogateKeyColumn;
    private String scd2ValidFromColumn;
    private String scd2ValidToColumn;
    private String scd2VersionColumn;
    private String scd2CurrentFlagColumn;
    private String partitioningColumns;
    private Integer jobPriority;
    private Integer maxRetries;
    private Long retryBackoffMs;
    private Integer slaThresholdMinutes;
    private String dependencyJobIds;
    private String auditLevel;
    private String reconciliationEnabled;
    private String reconciliationConfig; // CLOB as String
    private String dqChecksEnabled;
    private String dqRulesConfig; // CLOB as String
    private String notificationEmailsSuccess;
    private String notificationEmailsFailure;
    private String notificationVerbosity;
    private String jobProperties; // CLOB as String
    private String logMaskingColumns;
    private String executionAuthorizationFlag;
    private String manualTriggerOnly;
    private String createdBy;
    private Timestamp createdTs;
    private String updatedBy;
    private Timestamp updatedTs;
    private Integer configVersion;

    // Constructors (default and all-args, or use a builder pattern / Lombok for cleaner construction)
    public JobConfigDetailDTO() {}

    // Getters and Setters for all fields (essential for Jackson serialization/deserialization)
    // Example for a few fields:
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
