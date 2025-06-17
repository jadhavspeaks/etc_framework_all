package com.example.etl.api.dto;

import java.sql.Timestamp;

public class JobConfigAuditLogDTO {
    private Long logId;
    private String jobName;
    private String changeType;
    private String changedByUser;
    private Timestamp changeTimestamp;
    private String changedFieldsDetails; // JSON string

    // Constructors, Getters, Setters
    public JobConfigAuditLogDTO() {}

    public JobConfigAuditLogDTO(Long logId, String jobName, String changeType, String changedByUser, Timestamp changeTimestamp, String changedFieldsDetails) {
        this.logId = logId;
        this.jobName = jobName;
        this.changeType = changeType;
        this.changedByUser = changedByUser;
        this.changeTimestamp = changeTimestamp;
        this.changedFieldsDetails = changedFieldsDetails;
    }

    public Long getLogId() { return logId; }
    public void setLogId(Long logId) { this.logId = logId; }
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }
    public String getChangeType() { return changeType; }
    public void setChangeType(String changeType) { this.changeType = changeType; }
    public String getChangedByUser() { return changedByUser; }
    public void setChangedByUser(String changedByUser) { this.changedByUser = changedByUser; }
    public Timestamp getChangeTimestamp() { return changeTimestamp; }
    public void setChangeTimestamp(Timestamp changeTimestamp) { this.changeTimestamp = changeTimestamp; }
    public String getChangedFieldsDetails() { return changedFieldsDetails; }
    public void setChangedFieldsDetails(String changedFieldsDetails) { this.changedFieldsDetails = changedFieldsDetails; }
}
