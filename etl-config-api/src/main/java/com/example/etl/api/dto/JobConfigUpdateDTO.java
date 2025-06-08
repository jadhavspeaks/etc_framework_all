package com.example.etl.api.dto;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

public class JobConfigUpdateDTO {
    // jobName is typically taken from path variable and not updatable for PUT,
    // so it's often omitted from UpdateDTO or not validated here if present.

    @Size(max = 1000, message = "Job description cannot exceed 1000 characters.")
    private String jobDescription;

    @NotBlank(message = "is_active flag cannot be blank.")
    @Pattern(regexp = "^(Y|N)$", message = "is_active flag must be 'Y' or 'N'.")
    private String isActive;

    @NotBlank(message = "Source type cannot be blank.")
    @Size(max = 50, message = "Source type cannot exceed 50 characters.")
    private String sourceType;

    @Size(max = 4000, message = "Source connection details cannot exceed 4000 characters.")
    private String sourceConnectionDetails;

    @Size(max = 50, message = "Source format cannot exceed 50 characters.")
    private String sourceFormat;

    @Size(max = 1000, message = "Source format options cannot exceed 1000 characters.")
    private String sourceFormatOptions; // JSON String

    private String sourceSchema; // CLOB as String

    @NotBlank(message = "Target type cannot be blank.")
    @Size(max = 50, message = "Target type cannot exceed 50 characters.")
    private String targetType;

    @Size(max = 4000, message = "Target connection details cannot exceed 4000 characters.")
    private String targetConnectionDetails;

    @Size(max = 50, message = "Target format cannot exceed 50 characters.")
    private String targetFormat;

    @Size(max = 1000, message = "Target format options cannot exceed 1000 characters.")
    private String targetFormatOptions; // JSON String

    @NotBlank(message = "Target table or path cannot be blank.")
    @Size(max = 1000, message = "Target table or path cannot exceed 1000 characters.")
    private String targetTableOrPath;

    @NotBlank(message = "Load type cannot be blank.")
    @Pattern(regexp = "^(INCREMENTAL|FULL_RELOAD)$", message = "Load type must be 'INCREMENTAL' or 'FULL_RELOAD'.")
    private String loadType;

    @NotBlank(message = "Target write mode cannot be blank.")
    @Pattern(regexp = "^(overwrite|append|ignore|errorifexists)$", message = "Invalid target write mode.")
    private String targetWriteMode;

    @NotBlank(message = "Transformation mode cannot be blank.")
    @Pattern(regexp = "^(AS_IS|WITH_LOGIC|SCD2)$", message = "Invalid transformation mode.")
    private String transformationMode;

    private String sqlLogic;
    private String schemaMappingLogic;
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

    @NotBlank(message = "Audit level cannot be blank.")
    @Pattern(regexp = "^(NONE|JOB|RECORD)$", message = "Invalid audit level.")
    private String auditLevel;

    @NotBlank(message = "Reconciliation enabled flag cannot be blank.")
    @Pattern(regexp = "^(Y|N)$", message = "Reconciliation enabled flag must be 'Y' or 'N'.")
    private String reconciliationEnabled;
    private String reconciliationConfig;

    @NotBlank(message = "DQ checks enabled flag cannot be blank.")
    @Pattern(regexp = "^(Y|N)$", message = "DQ checks enabled flag must be 'Y' or 'N'.")
    private String dqChecksEnabled;
    private String dqRulesConfig;

    private String notificationEmailsSuccess;
    private String notificationEmailsFailure;
    @Pattern(regexp = "^(BRIEF|DETAILED)$", message = "Invalid notification verbosity.")
    private String notificationVerbosity;

    private String jobProperties;
    private String logMaskingColumns;

    @NotBlank(message = "Execution authorization flag cannot be blank.")
    @Pattern(regexp = "^(Y|N)$", message = "Execution authorization flag must be 'Y' or 'N'.")
    private String executionAuthorizationFlag;

    @NotBlank(message = "Manual trigger only flag cannot be blank.")
    @Pattern(regexp = "^(Y|N)$", message = "Manual trigger only flag must be 'Y' or 'N'.")
    private String manualTriggerOnly;

    // Getters and Setters
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
}
