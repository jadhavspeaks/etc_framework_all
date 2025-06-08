package com.example.etl.api.repository;

import com.example.etl.api.entity.JobConfigEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor; // For more complex dynamic queries later
import org.springframework.stereotype.Repository;

@Repository
public interface JobConfigRepository extends JpaRepository<JobConfigEntity, String>, JpaSpecificationExecutor<JobConfigEntity> {

    // Basic find by primary key (jobName) is provided by JpaRepository: findById(String jobName)

    // Example of a custom query method for filtering by job name (case-insensitive containment)
    Page<JobConfigEntity> findByJobNameContainingIgnoreCase(String jobNameFilter, Pageable pageable);

    // Example for filtering by source type (case-insensitive)
    Page<JobConfigEntity> findBySourceTypeIgnoreCase(String sourceType, Pageable pageable);

    // Example for filtering by both job name and source type
    Page<JobConfigEntity> findByJobNameContainingIgnoreCaseAndSourceTypeIgnoreCase(String jobNameFilter, String sourceType, Pageable pageable);

    // JpaSpecificationExecutor is added to allow for more complex, dynamic criteria later using Specifications if needed.
}
