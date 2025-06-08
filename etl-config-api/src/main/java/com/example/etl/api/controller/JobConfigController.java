package com.example.etl.api.controller;

import com.example.etl.api.dto.JobConfigCreateDTO;
import com.example.etl.api.dto.JobConfigDetailDTO;
import com.example.etl.api.dto.JobConfigSummaryDTO;
import com.example.etl.api.dto.JobConfigUpdateDTO;
import com.example.etl.api.service.JobConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.validation.Valid;
import java.net.URI;
import java.util.List; // For validation response

@RestController
@RequestMapping("/api/v1/jobconfigs")
public class JobConfigController {

    private final JobConfigService jobConfigService;

    @Autowired
    public JobConfigController(JobConfigService jobConfigService) {
        this.jobConfigService = jobConfigService;
    }

    @GetMapping
    public ResponseEntity<Page<JobConfigSummaryDTO>> getAllJobs(
            @RequestParam(required = false) String jobNameFilter,
            @RequestParam(required = false) String sourceTypeFilter,
            @PageableDefault(size = 20, sort = "jobName") Pageable pageable) {

        Page<JobConfigSummaryDTO> jobConfigs = jobConfigService.findAllJobs(
            pageable, jobNameFilter, sourceTypeFilter);
        return ResponseEntity.ok(jobConfigs);
    }

    @GetMapping("/{jobName}")
    public ResponseEntity<JobConfigDetailDTO> getJobByName(@PathVariable String jobName) {
        JobConfigDetailDTO jobConfig = jobConfigService.findJobByName(jobName);
        return ResponseEntity.ok(jobConfig);
    }

    @PostMapping
    public ResponseEntity<JobConfigDetailDTO> createJobConfig(
            @Valid @RequestBody JobConfigCreateDTO createDTO) {
        // TODO: Get username from Spring Security context once implemented
        String createdByUsername = "api-user"; // Placeholder
        JobConfigDetailDTO createdJob = jobConfigService.createJob(createDTO, createdByUsername);

        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
            .path("/{jobName}")
            .buildAndExpand(createdJob.getJobName())
            .toUri();

        return ResponseEntity.created(location).body(createdJob);
    }

    @PutMapping("/{jobName}")
    public ResponseEntity<JobConfigDetailDTO> updateJobConfig(
            @PathVariable String jobName,
            @Valid @RequestBody JobConfigUpdateDTO updateDTO) {
        // TODO: Get username from Spring Security context
        String updatedByUsername = "api-user"; // Placeholder
        JobConfigDetailDTO updatedJob = jobConfigService.updateJob(jobName, updateDTO, updatedByUsername);
        return ResponseEntity.ok(updatedJob);
    }

    @DeleteMapping("/{jobName}")
    public ResponseEntity<Void> deleteJobConfig(@PathVariable String jobName) {
        // TODO: Possibly get username for audit logging of delete operation if needed
        jobConfigService.deleteJob(jobName);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/validate") // New endpoint
    public ResponseEntity<List<String>> validateJobConfig(
            @Valid @RequestBody JobConfigCreateDTO dtoToValidate) {
        // @Valid already performs bean validations on dtoToValidate.
        // If those fail, Spring will return a 400 Bad Request before this method is called.
        // This service call performs additional cross-field and business logic validations.
        List<String> validationMessages = jobConfigService.validateConfigurationPayload(dtoToValidate);
        if (validationMessages.isEmpty()) {
            return ResponseEntity.ok(List.of("Configuration payload is valid.")); // Or just an empty list / success object
        } else {
            // Consider returning HttpStatus.BAD_REQUEST if we want to signal errors this way too,
            // but 200 OK with error messages in body is also a common pattern for validation endpoints.
            return ResponseEntity.ok(validationMessages);
        }
    }
}
