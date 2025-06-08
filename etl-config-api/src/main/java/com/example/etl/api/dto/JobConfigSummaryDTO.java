package com.example.etl.api.dto;

import java.sql.Timestamp; // Or java.time.Instant / OffsetDateTime preferred for modern APIs

public class JobConfigSummaryDTO {
    private String jobName;
    private String jobDescription;
    private String sourceType;
    private String targetType;
    private String transformationMode;
    private String isActive; // Or boolean
    private Timestamp updatedTs;

    // Constructors, Getters, and Setters
    public JobConfigSummaryDTO() {}

    public JobConfigSummaryDTO(String jobName, String jobDescription, String sourceType, String targetType, String transformationMode, String isActive, Timestamp updatedTs) {
        this.jobName = jobName;
        this.jobDescription = jobDescription;
        this.sourceType = sourceType;
        this.targetType = targetType;
        this.transformationMode = transformationMode;
        this.isActive = isActive;
        this.updatedTs = updatedTs;
    }

    // Getters
    public String getJobName() { return jobName; }
    public String getJobDescription() { return jobDescription; }
    public String getSourceType() { return sourceType; }
    public String getTargetType() { return targetType; }
    public String getTransformationMode() { return transformationMode; }
    public String getIsActive() { return isActive; }
    public Timestamp getUpdatedTs() { return updatedTs; }

    // Setters
    public void setJobName(String jobName) { this.jobName = jobName; }
    public void setJobDescription(String jobDescription) { this.jobDescription = jobDescription; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }
    public void setTargetType(String targetType) { this.targetType = targetType; }
    public void setTransformationMode(String transformationMode) { this.transformationMode = transformationMode; }
    public void setIsActive(String isActive) { this.isActive = isActive; }
    public void setUpdatedTs(Timestamp updatedTs) { this.updatedTs = updatedTs; }
}
