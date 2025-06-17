package com.example.etl.api.repository;

import com.example.etl.api.entity.JobConfigAuditLogEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobConfigAuditLogRepository extends JpaRepository<JobConfigAuditLogEntity, Long> {
    // Long is the type of logId (PK)
    // Additional query methods can be added here if needed, e.g., findByJobNameOrderByChangeTimestampDesc
    Page<JobConfigAuditLogEntity> findByJobNameOrderByChangeTimestampDesc(String jobName, Pageable pageable);
}
