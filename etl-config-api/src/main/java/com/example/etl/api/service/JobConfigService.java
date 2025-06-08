package com.example.etl.api.service;

import com.example.etl.api.dto.JobConfigCreateDTO;
import com.example.etl.api.dto.JobConfigDetailDTO;
import com.example.etl.api.dto.JobConfigSummaryDTO;
import com.example.etl.api.dto.JobConfigUpdateDTO;
import com.example.etl.api.entity.JobConfigEntity;
import com.example.etl.api.exception.ResourceConflictException;
import com.example.etl.api.exception.ResourceNotFoundException;
import com.example.etl.api.repository.JobConfigRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class JobConfigService {

    private final JobConfigRepository jobConfigRepository;
    private final JobConfigMapper jobConfigMapper;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Set<String> SUPPORTED_TRANSFORMATION_MODES = Set.of("AS_IS", "WITH_LOGIC", "SCD2");
    private static final Set<String> SUPPORTED_LOAD_TYPES = Set.of("INCREMENTAL", "FULL_RELOAD");
    private static final Set<String> SUPPORTED_WRITE_MODES = Set.of("overwrite", "append", "ignore", "errorifexists");
    private static final Set<String> SUPPORTED_SOURCE_TYPES = Set.of("FILE", "HIVE_TABLE");
    private static final Set<String> SUPPORTED_TARGET_TYPES = Set.of("FILE", "HDFS", "HIVE_TABLE");
    private static final Set<String> SUPPORTED_FILE_FORMATS = Set.of("CSV", "PARQUET", "JSON", "ORC", "TEXT", "AVRO");
    private static final Set<String> SUPPORTED_AUDIT_LEVELS = Set.of("NONE", "JOB", "RECORD");
    private static final Set<String> YES_NO_FLAGS = Set.of("Y", "N");

    @Autowired
    public JobConfigService(JobConfigRepository jobConfigRepository, JobConfigMapper jobConfigMapper) {
        this.jobConfigRepository = jobConfigRepository;
        this.jobConfigMapper = jobConfigMapper;
    }

    @Transactional(readOnly = true)
    public Page<JobConfigSummaryDTO> findAllJobs(
            Pageable pageable,
            String jobNameFilter,
            String sourceTypeFilter) {
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

    @Transactional
    public JobConfigDetailDTO createJob(JobConfigCreateDTO createDTO, String createdByUsername) {
        // DTO level validation is handled by @Valid in the controller.
        // Business validation specific to creation (e.g., uniqueness of jobName) is handled here.
        if (jobConfigRepository.existsById(createDTO.getJobName())) {
            throw new ResourceConflictException("Job configuration with name '" + createDTO.getJobName() + "' already exists.");
        }
        // Cross-field business logic validations that are NOT covered by DTO annotations and are critical for DB consistency
        // can be placed here. However, the validateConfigurationPayload is more for pre-flight checks via the /validate endpoint.
        // For example, if certain combinations of sourceType and targetType were disallowed system-wide:
        // List<String> crossFieldErrors = performCriticalCrossFieldValidations(createDTO);
        // if (!crossFieldErrors.isEmpty()) {
        //     throw new IllegalArgumentException("Critical cross-field validation failed: " + crossFieldErrors.mkString("; "));
        // }

        JobConfigEntity entity = jobConfigMapper.fromCreateDTO(createDTO, createdByUsername);
        JobConfigEntity savedEntity = jobConfigRepository.save(entity);
        return jobConfigMapper.toDetailDTO(savedEntity);
    }

    @Transactional
    public JobConfigDetailDTO updateJob(String jobName, JobConfigUpdateDTO updateDTO, String updatedByUsername) {
        JobConfigEntity existingEntity = jobConfigRepository.findById(jobName)
            .orElseThrow(() -> new ResourceNotFoundException("Job configuration not found with name: " + jobName));

        // Similar to createJob, @Valid in controller handles DTO field validation.
        // Critical cross-field business logic for update could go here.
        // List<String> crossFieldErrors = performCriticalCrossFieldValidationsForUpdate(updateDTO, existingEntity);
        // if (!crossFieldErrors.isEmpty()) {
        //     throw new IllegalArgumentException("Critical cross-field validation failed for update: " + crossFieldErrors.mkString("; "));
        // }

        jobConfigMapper.updateEntityFromUpdateDTO(updateDTO, existingEntity, updatedByUsername);
        JobConfigEntity updatedEntity = jobConfigRepository.save(existingEntity);
        return jobConfigMapper.toDetailDTO(updatedEntity);
    }

    @Transactional
    public void deleteJob(String jobName) {
        if (!jobConfigRepository.existsById(jobName)) {
            throw new ResourceNotFoundException("Job configuration not found with name: " + jobName + " for deletion.");
        }
        jobConfigRepository.deleteById(jobName);
    }

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
}
