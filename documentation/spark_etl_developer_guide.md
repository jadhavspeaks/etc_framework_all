# Spark ETL Framework - Developer Guide

## 1. Introduction
This guide provides information for developers working on or extending the Spark ETL Framework.
It covers the architecture, core components, and guidelines for adding new functionality.

## 2. Framework Architecture

### 2.1. Overview
The framework is a configuration-driven ETL application built using Scala and Apache Spark. Job definitions are stored in an Oracle database (`JOB_CONFIG` table) and control all aspects of ETL processes.

Key components include:
- **Main Application (`MainApp.scala`)**: Entry point, orchestrates job execution based on configuration.
- **Configuration Loading (`ConfigLoader.scala`)**: Loads job definitions from Oracle.
- **Ingestion Handlers (`handlers/`)**: Implement specific logic for different `transformation_mode` values (AS_IS, WITH_LOGIC, SCD2).
- **Core Services**: Utilities for SQL parsing, schema mapping, data quality, reconciliation, auditing, and notifications.
- **API (Separate Module `etl-config-api`)**: Spring Boot API for managing job configurations.
- **UI (Separate Module `etl-config-ui`)**: React-based UI for the API.

### 2.2. Job Execution Flow (Simplified)
1. `MainApp` receives `job_name`.
2. `ConfigLoader` fetches `JobConfig` from Oracle.
3. `JobConfigValidator` validates the configuration.
4. `MainApp` checks `is_active` flag.
5. `JobAuditor` logs job start (to Hive).
6. Appropriate `IngestionHandler` is invoked based on `transformation_mode`.
   - Handler reads source(s) (File, Hive Table).
   - Handler applies transformations (e.g., Schema Mapping, SQL Logic execution, SCD2 logic).
   - Handler writes to target(s) (File, Hive Table).
   - Handler (optionally) invokes Reconciliation and Data Quality services.
7. `JobAuditor` logs job success/failure (to Hive), including DQ/Recon reports if available.
8. `NotificationService` sends email alert.

## 3. Core Spark Application (`spark-etl-framework` module)

### 3.1. Configuration (`com.example.etl.config`)
- **`JobConfig.scala`**: Case class mirroring the `JOB_CONFIG` Oracle table. This is the primary model for job definitions within the Spark app.
- **`ConfigLoader.scala`**: Connects to Oracle via JDBC (configured through Spark Session) and loads a `JobConfig` instance for a given `job_name`.

### 3.2. Main Application (`com.example.etl.MainApp`)
- Parses command-line arguments (expects `job_name`).
- Initializes SparkSession with Hive support.
- Manages overall job lifecycle: loads config, validates config, checks `is_active`, invokes handlers, calls auditing and notification services.
- Handles top-level exceptions and sets appropriate exit codes.

### 3.3. Ingestion Handlers (`com.example.etl.ingestion.handlers`)
Handlers implement the `execute(jobConfig: JobConfig, spark: SparkSession): (Option[Long], Option[Long], Option[DataQualityReport])` method (or similar signature).
- **`AsIsIngestionHandler`**: For direct data movement with schema mapping. Uses `SchemaMapper`.
- **`WithLogicIngestionHandler`**: For SQL-based transformations. Uses `SqlParserService` to get a `LogicalPlan`, `LogicalPlanValidator`, and then `Dataset.ofRows(spark, logicalPlan)` for execution. Supports parameter substitution in SQL from Spark conf (`spark.etl.param.*`).
- **`Scd2LogicHandler`**: For Type 2 Slowly Changing Dimensions. Uses `SqlParserService` (if `sql_logic` is for source transformation), `Scd2LogicUtil` for core SCD logic, and appends results.

**To Create a New Handler:**
1. Create a new Scala object in the `handlers` package.
2. Define an `execute` method with the standard signature.
3. Implement logic for reading source(s), transforming data, and writing to target(s) based on `JobConfig`.
4. Integrate calls to `SchemaMapper`, `ReconciliationService`, `DataQualityService` as needed.
5. Update `MainApp.scala` to add a case for the new `transformation_mode` that invokes your handler.
6. Update `JobConfigValidator.scala` with any specific validation rules for the new mode.

### 3.4. Utility Services

#### 3.4.1. `SchemaMapper.scala` (`com.example.etl.util`)
- Parses `schema_mapping_logic` JSON from `JobConfig`.
- Applies column renames, type casts, default value imputation.
- Handles basic schema drift based on `job_properties` configuration.

#### 3.4.2. `SqlParserService.scala` (`com.example.etl.sql`)
- Uses `spark.sessionState.sqlParser.parsePlan(sql)` to convert a SQL string into Spark''s `LogicalPlan`.
- Handles parsing errors.

#### 3.4.3. `LogicalPlanValidator.scala` (`com.example.etl.sql`)
- Traverses a `LogicalPlan` to check for disallowed operations (e.g., DDL/DML in transformation SQL).

#### 3.4.4. `Scd2LogicUtil.scala` (`com.example.etl.logic`)
- Contains core logic for SCD Type 2 processing:
  - `categorizeAndDetectChanges`: Compares source and target to find new, changed, unchanged, deleted records.
  - `generateScdOutputRecords`: Creates the final set of records with SCD metadata columns populated.

#### 3.4.5. `JobAuditor.scala` (`com.example.etl.audit`)
- **TARGETS HIVE TABLES.**
- `AuditSummaryRecord`, `JobRunDetailRecord`, `RejectionEventRecord` case classes define schemas for Hive audit tables.
- Methods (`logJobStart`, `logJobSuccess`, `logJobFailure`, `logJobSkipped`, `logRejection`) construct DataFrames from these records and write to configured Hive tables (e.g., `etl_framework_audit.audit_summary`).
- Captures Spark App ID, serialized JobConfig, stack traces, and (soon) DQ/Recon reports.

#### 3.4.6. `ReconciliationService.scala` (`com.example.etl.reconciliation`)
- Performs post-load checks between source and target DataFrames based on `reconciliation_config` JSON.
- Supported checks: `record_count`, `checksum` (CRC32 sum per column), `column_sum` (for numeric columns).
- Generates a `ReconciliationReport`.

#### 3.4.7. `DataQualityService.scala` (`com.example.etl.dq`)
- Performs DQ checks on a target DataFrame based on `dq_rules_config` JSON.
- Supported checks: `NOT_NULL`, `ROW_COUNT`, `UNIQUE_CHECK`, `REGEX_MATCH`, `CUSTOM_SQL_EXPRESSION`.
- Generates a `DataQualityReport`. Handles `error_level` to determine if a rule failure should fail the job.

#### 3.4.8. `NotificationService.scala` (`com.example.etl.notification`)
- Sends HTML email notifications for job outcomes (SUCCESS, FAILED, SKIPPED_INACTIVE).
- Loads templates from resources, populates placeholders with job metrics.
- Uses SMTP settings from Spark configuration.

#### 3.4.9. `LoggingUtil.scala` (`com.example.etl.util`)
- Provides `maskDataFrame` to mask specified columns in DataFrames before logging samples, driven by `jobConfig.log_masking_columns`.

### 3.5. Build Process
- The Spark application is built using Maven (`pom.xml`).
- `mvn clean package` creates a fat JAR in the `target/` directory, which can be submitted using `spark-submit`.

## 4. API Module (`etl-config-api` module)
- Spring Boot application providing a REST API for `JOB_CONFIG` management.
- Uses Java, Spring Data JPA, Spring Security.
- See `JobConfigController`, `JobConfigService`, `JobConfigEntity`, `JobConfigRepository`, DTOs.
- Secured with Basic Auth and RBAC (Roles: ADMIN, EDITOR, VIEWER).
- Logs changes to `JOB_CONFIG_AUDIT_LOG` Oracle table.
- (Details on extending API would go here - e.g., adding new endpoints, DTOs, service logic).

## 5. UI Module (`etl-config-ui` module)
- React application (TypeScript, Material-UI) for interacting with the API.
- (Details on extending UI would go here - e.g., new components, pages, state management).

## 6. Coding Conventions (Examples)
- Scala: Standard Scala style guide (e.g., Databricks, community Scala style guide).
- Java: Standard Java conventions.
- Consistent logging using SLF4J (for API) or Log4j (for Spark app).
- Meaningful variable and method names.
- Comments for complex logic.

---
*This guide should be expanded with more details as the framework evolves.*
