package com.example.etl.api.service;

import com.example.etl.api.dto.JobConfigCreateDTO;
import com.example.etl.api.dto.JobConfigDetailDTO;
import com.example.etl.api.dto.JobConfigSummaryDTO;
import com.example.etl.api.dto.JobConfigUpdateDTO;
import com.example.etl.api.entity.JobConfigEntity;
import org.springframework.stereotype.Component;
import java.sql.Timestamp;
import java.time.Instant;

@Component
public class JobConfigMapper {

    public JobConfigSummaryDTO toSummaryDTO(JobConfigEntity entity) {
        if (entity == null) return null;
        return new JobConfigSummaryDTO(
            entity.getJobName(),
            entity.getJobDescription(),
            entity.getSourceType(),
            entity.getTargetType(),
            entity.getTransformationMode(),
            entity.getIsActive(),
            entity.getUpdatedTs()
        );
    }

    public JobConfigDetailDTO toDetailDTO(JobConfigEntity entity) {
        if (entity == null) return null;
        JobConfigDetailDTO dto = new JobConfigDetailDTO();
        dto.setJobName(entity.getJobName());
        dto.setJobDescription(entity.getJobDescription());
        dto.setIsActive(entity.getIsActive());
        dto.setSourceType(entity.getSourceType());
        dto.setSourceConnectionDetails(entity.getSourceConnectionDetails());
        dto.setSourceFormat(entity.getSourceFormat());
        dto.setSourceFormatOptions(entity.getSourceFormatOptions());
        dto.setSourceSchema(entity.getSourceSchema());
        dto.setTargetType(entity.getTargetType());
        dto.setTargetConnectionDetails(entity.getTargetConnectionDetails());
        dto.setTargetFormat(entity.getTargetFormat());
        dto.setTargetFormatOptions(entity.getTargetFormatOptions());
        dto.setTargetTableOrPath(entity.getTargetTableOrPath());
        dto.setLoadType(entity.getLoadType());
        dto.setTargetWriteMode(entity.getTargetWriteMode());
        dto.setTransformationMode(entity.getTransformationMode());
        dto.setSqlLogic(entity.getSqlLogic());
        dto.setSchemaMappingLogic(entity.getSchemaMappingLogic());
        dto.setScd2NaturalKeys(entity.getScd2NaturalKeys());
        dto.setScd2ChangeTrackingColumn(entity.getScd2ChangeTrackingColumn());
        dto.setScd2SurrogateKeyColumn(entity.getScd2SurrogateKeyColumn());
        dto.setScd2ValidFromColumn(entity.getScd2ValidFromColumn());
        dto.setScd2ValidToColumn(entity.getScd2ValidToColumn());
        dto.setScd2VersionColumn(entity.getScd2VersionColumn());
        dto.setScd2CurrentFlagColumn(entity.getScd2CurrentFlagColumn());
        dto.setPartitioningColumns(entity.getPartitioningColumns());
        dto.setJobPriority(entity.getJobPriority());
        dto.setMaxRetries(entity.getMaxRetries());
        dto.setRetryBackoffMs(entity.getRetryBackoffMs());
        dto.setSlaThresholdMinutes(entity.getSlaThresholdMinutes());
        dto.setDependencyJobIds(entity.getDependencyJobIds());
        dto.setAuditLevel(entity.getAuditLevel());
        dto.setReconciliationEnabled(entity.getReconciliationEnabled());
        dto.setReconciliationConfig(entity.getReconciliationConfig());
        dto.setDqChecksEnabled(entity.getDqChecksEnabled());
        dto.setDqRulesConfig(entity.getDqRulesConfig());
        dto.setNotificationEmailsSuccess(entity.getNotificationEmailsSuccess());
        dto.setNotificationEmailsFailure(entity.getNotificationEmailsFailure());
        dto.setNotificationVerbosity(entity.getNotificationVerbosity());
        dto.setJobProperties(entity.getJobProperties());
        dto.setLogMaskingColumns(entity.getLogMaskingColumns());
        dto.setExecutionAuthorizationFlag(entity.getExecutionAuthorizationFlag());
        dto.setManualTriggerOnly(entity.getManualTriggerOnly());
        dto.setCreatedBy(entity.getCreatedBy());
        dto.setCreatedTs(entity.getCreatedTs());
        dto.setUpdatedBy(entity.getUpdatedBy());
        dto.setUpdatedTs(entity.getUpdatedTs());
        dto.setConfigVersion(entity.getConfigVersion());
        return dto;
    }

    public JobConfigEntity fromCreateDTO(JobConfigCreateDTO dto, String createdByUsername) {
        if (dto == null) return null;
        JobConfigEntity entity = new JobConfigEntity();
        entity.setJobName(dto.getJobName());
        entity.setJobDescription(dto.getJobDescription());
        entity.setIsActive(dto.getIsActive());
        entity.setSourceType(dto.getSourceType());
        entity.setSourceConnectionDetails(dto.getSourceConnectionDetails());
        entity.setSourceFormat(dto.getSourceFormat());
        entity.setSourceFormatOptions(dto.getSourceFormatOptions());
        entity.setSourceSchema(dto.getSourceSchema());
        entity.setTargetType(dto.getTargetType());
        entity.setTargetConnectionDetails(dto.getTargetConnectionDetails());
        entity.setTargetFormat(dto.getTargetFormat());
        entity.setTargetFormatOptions(dto.getTargetFormatOptions());
        entity.setTargetTableOrPath(dto.getTargetTableOrPath());
        entity.setLoadType(dto.getLoadType());
        entity.setTargetWriteMode(dto.getTargetWriteMode());
        entity.setTransformationMode(dto.getTransformationMode());
        entity.setSqlLogic(dto.getSqlLogic());
        entity.setSchemaMappingLogic(dto.getSchemaMappingLogic());
        entity.setScd2NaturalKeys(dto.getScd2NaturalKeys());
        entity.setScd2ChangeTrackingColumn(dto.getScd2ChangeTrackingColumn());
        entity.setScd2SurrogateKeyColumn(dto.getScd2SurrogateKeyColumn());
        entity.setScd2ValidFromColumn(dto.getScd2ValidFromColumn());
        entity.setScd2ValidToColumn(dto.getScd2ValidToColumn());
        entity.setScd2VersionColumn(dto.getScd2VersionColumn());
        entity.setScd2CurrentFlagColumn(dto.getScd2CurrentFlagColumn());
        entity.setPartitioningColumns(dto.getPartitioningColumns());
        entity.setJobPriority(dto.getJobPriority());
        entity.setMaxRetries(dto.getMaxRetries());
        entity.setRetryBackoffMs(dto.getRetryBackoffMs());
        entity.setSlaThresholdMinutes(dto.getSlaThresholdMinutes());
        entity.setDependencyJobIds(dto.getDependencyJobIds());
        entity.setAuditLevel(dto.getAuditLevel());
        entity.setReconciliationEnabled(dto.getReconciliationEnabled());
        entity.setReconciliationConfig(dto.getReconciliationConfig());
        entity.setDqChecksEnabled(dto.getDqChecksEnabled());
        entity.setDqRulesConfig(dto.getDqRulesConfig());
        entity.setNotificationEmailsSuccess(dto.getNotificationEmailsSuccess());
        entity.setNotificationEmailsFailure(dto.getNotificationEmailsFailure());
        entity.setNotificationVerbosity(dto.getNotificationVerbosity());
        entity.setJobProperties(dto.getJobProperties());
        entity.setLogMaskingColumns(dto.getLogMaskingColumns());
        entity.setExecutionAuthorizationFlag(dto.getExecutionAuthorizationFlag());
        entity.setManualTriggerOnly(dto.getManualTriggerOnly());

        Timestamp now = Timestamp.from(Instant.now());
        entity.setCreatedBy(createdByUsername);
        entity.setCreatedTs(now);
        entity.setUpdatedBy(createdByUsername);
        entity.setUpdatedTs(now);
        entity.setConfigVersion(1); // Initial version

        return entity;
    }

    public void updateEntityFromUpdateDTO(JobConfigUpdateDTO dto, JobConfigEntity entity, String updatedByUsername) {
        if (dto == null || entity == null) return;

        // Update only fields present in UpdateDTO (or all if it's a full update DTO)
        // Assuming UpdateDTO contains all fields that can be updated.
        entity.setJobDescription(dto.getJobDescription());
        entity.setIsActive(dto.getIsActive());
        entity.setSourceType(dto.getSourceType());
        entity.setSourceConnectionDetails(dto.getSourceConnectionDetails());
        entity.setSourceFormat(dto.getSourceFormat());
        entity.setSourceFormatOptions(dto.getSourceFormatOptions());
        entity.setSourceSchema(dto.getSourceSchema());
        entity.setTargetType(dto.getTargetType());
        entity.setTargetConnectionDetails(dto.getTargetConnectionDetails());
        entity.setTargetFormat(dto.getTargetFormat());
        entity.setTargetFormatOptions(dto.getTargetFormatOptions());
        entity.setTargetTableOrPath(dto.getTargetTableOrPath());
        entity.setLoadType(dto.getLoadType());
        entity.setTargetWriteMode(dto.getTargetWriteMode());
        entity.setTransformationMode(dto.getTransformationMode());
        entity.setSqlLogic(dto.getSqlLogic());
        entity.setSchemaMappingLogic(dto.getSchemaMappingLogic());
        entity.setScd2NaturalKeys(dto.getScd2NaturalKeys());
        entity.setScd2ChangeTrackingColumn(dto.getScd2ChangeTrackingColumn());
        entity.setScd2SurrogateKeyColumn(dto.getScd2SurrogateKeyColumn());
        entity.setScd2ValidFromColumn(dto.getScd2ValidFromColumn());
        entity.setScd2ValidToColumn(dto.getScd2ValidToColumn());
        entity.setScd2VersionColumn(dto.getScd2VersionColumn());
        entity.setScd2CurrentFlagColumn(dto.getScd2CurrentFlagColumn());
        entity.setPartitioningColumns(dto.getPartitioningColumns());
        entity.setJobPriority(dto.getJobPriority());
        entity.setMaxRetries(dto.getMaxRetries());
        entity.setRetryBackoffMs(dto.getRetryBackoffMs());
        entity.setSlaThresholdMinutes(dto.getSlaThresholdMinutes());
        entity.setDependencyJobIds(dto.getDependencyJobIds());
        entity.setAuditLevel(dto.getAuditLevel());
        entity.setReconciliationEnabled(dto.getReconciliationEnabled());
        entity.setReconciliationConfig(dto.getReconciliationConfig());
        entity.setDqChecksEnabled(dto.getDqChecksEnabled());
        entity.setDqRulesConfig(dto.getDqRulesConfig());
        entity.setNotificationEmailsSuccess(dto.getNotificationEmailsSuccess());
        entity.setNotificationEmailsFailure(dto.getNotificationEmailsFailure());
        entity.setNotificationVerbosity(dto.getNotificationVerbosity());
        entity.setJobProperties(dto.getJobProperties());
        entity.setLogMaskingColumns(dto.getLogMaskingColumns());
        entity.setExecutionAuthorizationFlag(dto.getExecutionAuthorizationFlag());
        entity.setManualTriggerOnly(dto.getManualTriggerOnly());

        entity.setUpdatedBy(updatedByUsername);
        entity.setUpdatedTs(Timestamp.from(Instant.now()));
        if (entity.getConfigVersion() != null) {
            entity.setConfigVersion(entity.getConfigVersion() + 1);
        } else {
            entity.setConfigVersion(1); // Should not happen if created correctly
        }
    }
}
