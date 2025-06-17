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
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.validation.Valid;
import com.example.etl.api.dto.JobConfigAuditLogDTO; // Added import
import org.springframework.data.domain.Sort; // Added import for Sort.Direction
import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/v1/jobconfigs")
public class JobConfigController {

    private final JobConfigService jobConfigService;

    @Autowired
    public JobConfigController(JobConfigService jobConfigService) {
        this.jobConfigService = jobConfigService;
    }

    @GetMapping
    @PreAuthorize("hasAnyRole('VIEWER', 'EDITOR', 'ADMIN')")
    public ResponseEntity<Page<JobConfigSummaryDTO>> getAllJobs(
            @RequestParam(required = false) String jobNameFilter,
            @RequestParam(required = false) String sourceTypeFilter,
            @PageableDefault(size = 20, sort = "jobName") Pageable pageable) {

        Page<JobConfigSummaryDTO> jobConfigs = jobConfigService.findAllJobs(
            pageable, jobNameFilter, sourceTypeFilter);
        return ResponseEntity.ok(jobConfigs);
    }

    @GetMapping("/{jobName}")
    @PreAuthorize("hasAnyRole('VIEWER', 'EDITOR', 'ADMIN')")
    public ResponseEntity<JobConfigDetailDTO> getJobByName(@PathVariable String jobName) {
        JobConfigDetailDTO jobConfig = jobConfigService.findJobByName(jobName);
        return ResponseEntity.ok(jobConfig);
    }

    @PostMapping
    @PreAuthorize("hasAnyRole('EDITOR', 'ADMIN')")
    public ResponseEntity<JobConfigDetailDTO> createJobConfig(
            @Valid @RequestBody JobConfigCreateDTO createDTO,
            @AuthenticationPrincipal UserDetails userDetails) {

        String username = (userDetails != null) ? userDetails.getUsername() : "anonymousApiUser";
        JobConfigDetailDTO createdJob = jobConfigService.createJob(createDTO, username);

        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
            .path("/{jobName}")
            .buildAndExpand(createdJob.getJobName())
            .toUri();

        return ResponseEntity.created(location).body(createdJob);
    }

    @PutMapping("/{jobName}")
    @PreAuthorize("hasAnyRole('EDITOR', 'ADMIN')")
    public ResponseEntity<JobConfigDetailDTO> updateJobConfig(
            @PathVariable String jobName,
            @Valid @RequestBody JobConfigUpdateDTO updateDTO,
            @AuthenticationPrincipal UserDetails userDetails) {

        String username = (userDetails != null) ? userDetails.getUsername() : "anonymousApiUser";
        JobConfigDetailDTO updatedJob = jobConfigService.updateJob(jobName, updateDTO, username);
        return ResponseEntity.ok(updatedJob);
    }

    @DeleteMapping("/{jobName}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<Void> deleteJobConfig(
            @PathVariable String jobName,
            @AuthenticationPrincipal UserDetails userDetails) { // Added UserDetails

        String username = (userDetails != null) ? userDetails.getUsername() : "anonymousApiUser";
        jobConfigService.deleteJob(jobName, username); // Pass username to service
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/validate")
    @PreAuthorize("hasAnyRole('EDITOR', 'ADMIN')")
    public ResponseEntity<List<String>> validateJobConfig(
            @Valid @RequestBody JobConfigCreateDTO dtoToValidate) {
        List<String> validationMessages = jobConfigService.validateConfigurationPayload(dtoToValidate);
        if (validationMessages.isEmpty()) {
            return ResponseEntity.ok(List.of("Configuration payload is valid."));
        } else {
            return ResponseEntity.ok(validationMessages);
        }
    }

    // Add this method to the existing class
    @GetMapping("/{jobName}/auditlogs")
    @PreAuthorize("hasAnyRole('EDITOR', 'ADMIN')") // Or appropriate role
    public ResponseEntity<Page<JobConfigAuditLogDTO>> getJobAuditLogs(
            @PathVariable String jobName,
            @PageableDefault(size = 10, sort = "changeTimestamp", direction = Sort.Direction.DESC) Pageable pageable) {
        Page<JobConfigAuditLogDTO> auditLogs = jobConfigService.findAuditLogsByJobName(jobName, pageable);
        return ResponseEntity.ok(auditLogs);
    }
}
