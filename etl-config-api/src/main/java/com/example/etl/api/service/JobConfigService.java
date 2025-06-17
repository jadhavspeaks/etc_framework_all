package com.example.etl.api.service;

import com.example.etl.api.dto.JobConfigCreateDTO;
import com.example.etl.api.dto.JobConfigDetailDTO;
import com.example.etl.api.dto.JobConfigSummaryDTO;
import com.example.etl.api.dto.JobConfigUpdateDTO;
import com.example.etl.api.entity.JobConfigAuditLogEntity;
import com.example.etl.api.entity.JobConfigEntity;
import com.example.etl.api.exception.ResourceConflictException;
import com.example.etl.api.exception.ResourceNotFoundException;
import com.example.etl.api.repository.JobConfigAuditLogRepository;
import com.example.etl.api.repository.JobConfigRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger; // Using SLF4J for logging
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class JobConfigService {

    private static final Logger logger = LoggerFactory.getLogger(JobConfigService.class);

    private final JobConfigRepository jobConfigRepository;
    private final JobConfigAuditLogRepository jobConfigAuditLogRepository; // Added
    private final JobConfigMapper jobConfigMapper;
    private final ObjectMapper objectMapper; // Re-use existing or Autowire if configured as Bean

    // Supported value sets remain the same...
    private static final Set<String> SUPPORTED_TRANSFORMATION_MODES = Set.of("AS_IS", "WITH_LOGIC", "SCD2");
    private static final Set<String> SUPPORTED_LOAD_TYPES = Set.of("INCREMENTAL", "FULL_RELOAD");
    private static final Set<String> SUPPORTED_WRITE_MODES = Set.of("overwrite", "append", "ignore", "errorifexists");
    private static final Set<String> SUPPORTED_SOURCE_TYPES = Set.of("FILE", "HIVE_TABLE");
    private static final Set<String> SUPPORTED_TARGET_TYPES = Set.of("FILE", "HDFS", "HIVE_TABLE");
    private static final Set<String> SUPPORTED_FILE_FORMATS = Set.of("CSV", "PARQUET", "JSON", "ORC", "TEXT", "AVRO");

    @Autowired
    public JobConfigService(JobConfigRepository jobConfigRepository,
                            JobConfigAuditLogRepository jobConfigAuditLogRepository, // Added
                            JobConfigMapper jobConfigMapper,
                            ObjectMapper objectMapper) { // Added/assuming ObjectMapper bean
        this.jobConfigRepository = jobConfigRepository;
        this.jobConfigAuditLogRepository = jobConfigAuditLogRepository;
        this.jobConfigMapper = jobConfigMapper;
        this.objectMapper = objectMapper;
    }

    // Read methods (findAllJobs, findJobByName) remain the same...
    @Transactional(readOnly = true)
    public Page<JobConfigSummaryDTO> findAllJobs(Pageable pageable, String jobNameFilter, String sourceTypeFilter) {
        Page<JobConfigEntity> pageResult;
        boolean hasJobNameFilter = jobNameFilter != null && !jobNameFilter.trim().isEmpty();
        boolean hasSourceTypeFilter = sourceTypeFilter != null && !sourceTypeFilter.trim().isEmpty();
        if (hasJobNameFilter && hasSourceTypeFilter) {
            pageResult = jobConfigRepository.findByJobNameContainingIgnoreCaseAndSourceTypeIgnoreCase(jobNameFilter, sourceTypeFilter, pageable);
        } else if (hasJobNameFilter) {
            pageResult = jobConfigRepository.findByJobNameContainingIgnoreCase(jobNameFilter, pageable);
        } else if (hasSourceTypeFilter) {
            pageResult = jobConfigRepository.findBySourceTypeIgnoreCase(sourceTypeFilter, pageable);
        } else {
            pageResult = jobConfigRepository.findAll(pageable);
        }
        return pageResult.map(jobConfigMapper::toSummaryDTO);
    }

    @Transactional(readOnly = true)
    public JobConfigDetailDTO findJobByName(String jobName) {
        JobConfigEntity entity = jobConfigRepository.findById(jobName)
            .orElseThrow(() -> new ResourceNotFoundException("Job configuration not found with name: " + jobName));
        return jobConfigMapper.toDetailDTO(entity);
    }

    private String convertToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.warn("Could not serialize object to JSON for audit log: " + e.getMessage(), e);
            return "{\"error\": \"Could not serialize details to JSON\"}";
        }
    }

    @Transactional
    public JobConfigDetailDTO createJob(JobConfigCreateDTO createDTO, String createdByUsername) {
        if (jobConfigRepository.existsById(createDTO.getJobName())) {
            throw new ResourceConflictException("Job configuration with name '" + createDTO.getJobName() + "' already exists.");
        }
        JobConfigEntity entity = jobConfigMapper.fromCreateDTO(createDTO, createdByUsername);
        JobConfigEntity savedEntity = jobConfigRepository.save(entity);

        // Audit Log for CREATE
        JobConfigAuditLogEntity auditLog = new JobConfigAuditLogEntity(
            savedEntity.getJobName(),
            "CREATE",
            createdByUsername,
            Timestamp.from(Instant.now()),
            convertToJson(jobConfigMapper.toDetailDTO(savedEntity)) // Log the full created entity details
        );
        jobConfigAuditLogRepository.save(auditLog);

        return jobConfigMapper.toDetailDTO(savedEntity);
    }

    @Transactional
    public JobConfigDetailDTO updateJob(String jobName, JobConfigUpdateDTO updateDTO, String updatedByUsername) {
        JobConfigEntity existingEntity = jobConfigRepository.findById(jobName)
            .orElseThrow(() -> new ResourceNotFoundException("Job configuration not found with name: " + jobName));

        // For detailed field diff, would need to capture state before mapping update
        // String oldDetailsJson = convertToJson(jobConfigMapper.toDetailDTO(existingEntity)); // If needed

        jobConfigMapper.updateEntityFromUpdateDTO(updateDTO, existingEntity, updatedByUsername);
        JobConfigEntity updatedEntity = jobConfigRepository.save(existingEntity);

        // Audit Log for UPDATE
        JobConfigAuditLogEntity auditLog = new JobConfigAuditLogEntity(
            updatedEntity.getJobName(),
            "UPDATE",
            updatedByUsername,
            Timestamp.from(Instant.now()),
            convertToJson(jobConfigMapper.toDetailDTO(updatedEntity)) // Log the full new state
            // For detailed diff: store oldDetailsJson and newDetailsJson or a computed diff
        );
        jobConfigAuditLogRepository.save(auditLog);

        return jobConfigMapper.toDetailDTO(updatedEntity);
    }

    @Transactional
    public void deleteJob(String jobName, String deletedByUsername) { // Added deletedByUsername for audit
        JobConfigEntity entityToDelete = jobConfigRepository.findById(jobName)
            .orElseThrow(() -> new ResourceNotFoundException("Job configuration not found with name: " + jobName + " for deletion."));

        // Audit Log for DELETE - log details before deleting
        JobConfigAuditLogEntity auditLog = new JobConfigAuditLogEntity(
            entityToDelete.getJobName(),
            "DELETE",
            deletedByUsername,
            Timestamp.from(Instant.now()),
            convertToJson(jobConfigMapper.toDetailDTO(entityToDelete)) // Log the details of the entity being deleted
        );
        jobConfigAuditLogRepository.save(auditLog);

        jobConfigRepository.deleteById(jobName);
    }

    // Validation method (validateConfigurationPayload) remains the same...
    public List<String> validateConfigurationPayload(JobConfigCreateDTO dto) {
        List<String> errors = new ArrayList<>();
        if (dto.getSourceType() != null && !SUPPORTED_SOURCE_TYPES.contains(dto.getSourceType().toUpperCase())) {
            errors.add("Unsupported source_type: '" + dto.getSourceType() + "'. Supported: " + SUPPORTED_SOURCE_TYPES);
        }
        if (dto.getTargetType() != null && !SUPPORTED_TARGET_TYPES.contains(dto.getTargetType().toUpperCase())) {
            errors.add("Unsupported target_type: '" + dto.getTargetType() + "'. Supported: " + SUPPORTED_TARGET_TYPES);
        }
        if ("FILE".equalsIgnoreCase(dto.getSourceType())) {
            if (dto.getSourceFormat() == null || dto.getSourceFormat().trim().isEmpty()) {
                errors.add("source_format is mandatory for source_type 'FILE'.");
            } else if (!SUPPORTED_FILE_FORMATS.contains(dto.getSourceFormat().toUpperCase())){
                 errors.add("Unsupported source_format: '" + dto.getSourceFormat() + "'.");
            }
            if (dto.getSourceConnectionDetails() == null || dto.getSourceConnectionDetails().trim().isEmpty()) {
                errors.add("source_connection_details (as path) is mandatory for source_type 'FILE'.");
            }
        } else if ("HIVE_TABLE".equalsIgnoreCase(dto.getSourceType())) {
            if (dto.getSourceConnectionDetails() == null || dto.getSourceConnectionDetails().trim().isEmpty()) {
                errors.add("source_connection_details (as db.table) is mandatory for source_type 'HIVE_TABLE'.");
            }
            if (dto.getSourceFormat() != null && !dto.getSourceFormat().trim().isEmpty()) {
                errors.add("source_format should not be defined for source_type 'HIVE_TABLE'.");
            }
        }
        String transformationModeUpper = (dto.getTransformationMode() != null) ? dto.getTransformationMode().toUpperCase() : "";
        if ("WITH_LOGIC".equals(transformationModeUpper) || "SCD2".equals(transformationModeUpper)) {
            if (dto.getSqlLogic() == null || dto.getSqlLogic().trim().isEmpty()) {
                errors.add("sql_logic is mandatory and cannot be empty for transformation_mode '" + dto.getTransformationMode() + "'.");
            }
        }
        if ("SCD2".equals(transformationModeUpper)) {
            if (dto.getScd2NaturalKeys() == null || dto.getScd2NaturalKeys().trim().isEmpty()) {
                errors.add("scd2_natural_keys is mandatory and cannot be empty for transformation_mode 'SCD2'.");
            }
        }
        validateJsonStringStructure(dto.getSchemaMappingLogic(), "schema_mapping_logic", errors, false);
        validateJsonStringStructure(dto.getSourceFormatOptions(), "source_format_options", errors, true);
        validateJsonStringStructure(dto.getTargetFormatOptions(), "target_format_options", errors, true);
        validateJsonStringStructure(dto.getReconciliationConfig(), "reconciliation_config", errors, true);
        validateJsonStringStructure(dto.getDqRulesConfig(), "dq_rules_config", errors, false);
        validateJsonStringStructure(dto.getJobProperties(), "job_properties", errors, true);
        return errors;
    }

    private void validateJsonStringStructure(String jsonString, String fieldName, List<String> errors, boolean mustBeMap) {
        if (jsonString != null && !jsonString.trim().isEmpty()) {
            try {
                Object parsed = objectMapper.readValue(jsonString, Object.class);
                if (mustBeMap && !(parsed instanceof java.util.Map)) {
                    errors.add(fieldName + " must be a valid JSON object (e.g. {} or {\"key\":\"value\"}).");
                }
            } catch (JsonProcessingException e) {
                errors.add(fieldName + " is not valid JSON: " + e.getMessage());
            }
        }
    }

    // Add this method to the existing class
    @Transactional(readOnly = true)
    public Page<JobConfigAuditLogDTO> findAuditLogsByJobName(String jobName, Pageable pageable) {
        Page<JobConfigAuditLogEntity> auditLogPage = jobConfigAuditLogRepository.findByJobNameOrderByChangeTimestampDesc(jobName, pageable);
        if (auditLogPage.isEmpty() && !jobConfigRepository.existsById(jobName)) {
            // If the main job config doesn't exist, it's a 404 for the parent resource
            throw new ResourceNotFoundException("Job configuration not found with name: " + jobName + ", cannot retrieve audit logs.");
        }
        return auditLogPage.map(jobConfigMapper::toJobConfigAuditLogDTO);
    }
}
