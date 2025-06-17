# Enterprise Scala Spark ETL Framework

## 1. Overview

This project is a comprehensive, configuration-driven ETL framework built using Scala and Apache Spark for the backend processing engine. It features a Spring Boot (Java) REST API for managing job configurations and a React (TypeScript) web UI for user interaction. The entire framework is designed for production readiness, scalability, and ease of use, supporting various ingestion modes and advanced data governance features.

Job configurations are stored centrally in an Oracle database, and detailed job auditing is performed using Hive tables.

## 2. Features

*   **Configuration-Driven**: All ETL jobs are defined and managed via metadata in an Oracle database (`JOB_CONFIG` table).
*   **Multiple Ingestion Modes**:
    *   **AS_IS**: Direct data movement with schema mapping and validation.
    *   **WITH_LOGIC**: Custom SQL-based transformations (parsed and executed as Spark LogicalPlans), supporting runtime parameterization.
    *   **SCD Type 2**: Slowly Changing Dimension (Type 2) handling for dimension table management.
*   **Advanced Schema Mapping**: Supports column renaming, type casting, default value imputation, and configurable schema drift handling.
*   **Data Validation**: Pre-execution validation of job configurations and post-load Data Quality checks on target data.
*   **Reconciliation**: Post-load reconciliation of source vs. target (record counts, checksums, column sums).
*   **Comprehensive Auditing**: Job run summaries, detailed run information (Spark App ID, config used, errors), and data rejection events are logged to Hive tables.
*   **Notifications**: HTML email notifications for job success, failure, and skipped (inactive) status.
*   **REST API**: Secure Spring Boot API for CRUD operations on job configurations, including validation and change auditing.
*   **Web UI**: React-based user interface for managing job configurations through the API, with login and RBAC.
*   **Operational Features**: Log masking for sensitive data (in debug logs), `is_active` flag for jobs, parameterization for orchestrator integration.

## 3. High-Level Architecture

*(Placeholder: An architecture diagram here would visually represent the components below and their interactions.)*

The framework consists of three main deployable components and two primary databases:

1.  **Spark ETL Framework (`spark-etl-framework`)**: The core Scala/Spark application that executes ETL jobs.
2.  **Configuration API (`etl-config-api`)**: A Spring Boot REST API that provides an interface to manage job configurations stored in Oracle.
3.  **Configuration UI (`etl-config-ui`)**: A React-based single-page application that consumes the Configuration API to provide a user interface for job management.
4.  **Oracle Database**: Stores the `JOB_CONFIG` table (master definitions) and `JOB_CONFIG_AUDIT_LOG` (tracks changes to configurations made via API).
5.  **Apache Hive**: Stores audit tables (`audit_summary`, `job_run_details`, `rejection_events`) populated by the Spark ETL Framework.

## 4. Repository Structure
```
/
|-- spark-etl-framework/      # Scala/Spark ETL engine (Maven project)
|   |-- src/main/scala/com/example/etl/...
|   |-- pom.xml
|-- etl-config-api/           # Spring Boot REST API for configuration (Maven project)
|   |-- src/main/java/com/example/etl/api/...
|   |-- pom.xml
|   |-- src/main/resources/application.properties
|-- etl-config-ui/            # React frontend UI (npm/yarn project)
|   |-- src/...
|   |-- public/
|   |-- package.json
|-- ddl/                      # Database Definition Language files
|   |-- oracle_job_config_ddl.sql
|   |-- oracle_job_config_audit_log_ddl.sql
|   |-- hive_audit_summary_table.hql
|   |-- hive_job_run_details_table.hql
|   |-- hive_rejection_events_table.hql
|-- documentation/            # Markdown documentation files
|   |-- oracle_config_schema.md
|   |-- spark_etl_developer_guide.md
|   |-- operations_manual.md
|   |-- job_configuration_examples.md
|-- README.md                 # This file
```

## 5. Backend ETL Framework (`spark-etl-framework`)

This is the core engine responsible for executing the ETL jobs.

### 5.1. Setup & Prerequisites
- Java (JDK 8 or 11 compatible with Spark 2.4.x)
- Scala (2.11.12, as per `pom.xml`)
- Apache Maven (for building)
- Apache Spark (2.4.8, with Hive support enabled in Spark runtime)
- Access to an Oracle database (for job configurations).
- Access to a Hive metastore and HDFS (for Hive audit tables and potentially for source/target data).

### 5.2. Configuration (Runtime via `spark-submit --conf`)
Key runtime configurations are passed to `MainApp.scala` using the `--conf` option of `spark-submit`.
- **Oracle Connection**:
  - `spark.etl.oracle.url`, `spark.etl.oracle.user`, `spark.etl.oracle.password`
- **SMTP for Notifications**:
  - `spark.etl.smtp.host`, `spark.etl.smtp.port`, `spark.etl.smtp.user`, `spark.etl.smtp.password`, `spark.etl.smtp.from`, etc.
- **Hive Auditing**: Table names are currently hardcoded in `JobAuditor.scala` (e.g., `etl_framework_audit.audit_summary`). Ensure the database `etl_framework_audit` exists in Hive.
- **Custom Job Parameters**: For use in `sql_logic`, pass as `spark.etl.param.your_param_name=your_value`.

*(Refer to `documentation/operations_manual.md` for a more detailed list of configurations.)*

### 5.3. Building the JAR
Navigate to the `spark-etl-framework` directory and run:
```bash
mvn clean package
```
This will produce a fat JAR (e.g., `spark-etl-framework-1.0-SNAPSHOT-jar-with-dependencies.jar`) in the `target/` directory.

### 5.4. Running a Job
Jobs are submitted using `spark-submit`.
```bash
spark-submit \
    --class com.example.etl.MainApp \
    --master yarn \
    --name "ETL Job: <JOB_NAME>" \
    --conf spark.etl.oracle.url=...
    # ... other necessary --conf parameters ...
    /path/to/spark-etl-framework-VERSION-jar-with-dependencies.jar \
    <JOB_NAME>
```
- The `<JOB_NAME>` argument corresponds to the `job_name` in the `JOB_CONFIG` Oracle table.

*(Refer to `documentation/operations_manual.md` for a detailed `spark-submit` example.)*

### 5.5. Key Modules Overview
- **MainApp**: Entry point, job orchestration.
- **Handlers (`AsIs`, `WithLogic`, `SCD2`)**: Implement specific ingestion/transformation logic.
- **Services (`SchemaMapper`, `SqlParserService`, `LogicalPlanValidator`, `Scd2LogicUtil`, `JobAuditor`, `ReconciliationService`, `DataQualityService`, `NotificationService`, `LoggingUtil`)**: Provide reusable functionalities.

*(Refer to `documentation/spark_etl_developer_guide.md` for details on these modules.)*

## 6. Backend API (`etl-config-api`)

Provides RESTful services for managing job configurations.

### 6.1. Setup & Prerequisites
- Java (JDK 11, as per `pom.xml`)
- Apache Maven
- Access to the Oracle database (for `JOB_CONFIG` and `JOB_CONFIG_AUDIT_LOG` tables).

### 6.2. Configuration
- Database connection and other application settings are in `etl-config-api/src/main/resources/application.properties`.
- Update placeholders for Oracle URL, username, and password.

### 6.3. Building and Running the API
Navigate to the `etl-config-api` directory:
- **Build**: `mvn clean package` (creates a JAR in `target/`)
- **Run**: `java -jar target/etl-config-api-0.0.1-SNAPSHOT.jar` (or run from IDE).

The API will typically start on port 8080 (or as configured).

### 6.4. API Endpoints & Documentation
- Once the API is running, Swagger UI is available at: `http://localhost:8080/swagger-ui.html`
- This provides interactive documentation for all API endpoints.
- Key base path: `/api/v1/jobconfigs`

### 6.5. Security
- **Authentication**: HTTP Basic Authentication.
- **Authorization**: Role-Based Access Control (RBAC).
  - **In-memory test users (defined in `SecurityConfig.java`)**:
    - `admin` / `adminpass` (Roles: ADMIN, EDITOR, VIEWER)
    - `editor` / `editorpass` (Roles: EDITOR, VIEWER)
    - `viewer` / `viewerpass` (Roles: VIEWER)
  - Role management is considered external for production.

## 7. Frontend UI (`etl-config-ui`)

A React-based Single Page Application (SPA) for interacting with the Configuration API.

### 7.1. Setup & Prerequisites
- Node.js and npm (or yarn).

### 7.2. Configuration
- The API base URL can be configured via the `REACT_APP_API_BASE_URL` environment variable. If not set, it defaults to `http://localhost:8080/api/v1` (defined in `src/services/apiClient.ts`).
- To set it during development, create a `.env` file in the `etl-config-ui` root with:
  `REACT_APP_API_BASE_URL=http://your-api-host:port/api/v1`

### 7.3. Building and Serving the UI
Navigate to the `etl-config-ui` directory:
- **Install dependencies**: `npm install` (or `yarn install`)
- **Run development server**: `npm start` (or `yarn start`). Usually opens at `http://localhost:3000`.
- **Build for production**: `npm run build` (or `yarn build`). Creates optimized static assets in the `build/` directory.

### 7.4. Key UI Features
- Login page (uses credentials defined in the API''s `SecurityConfig`).
- List, view, create, edit, and delete ETL job configurations.
- Pre-flight validation of configurations before saving.
- View audit trail of changes for each job configuration.
- UI elements and actions are controlled by user roles (ADMIN, EDITOR, VIEWER).

*(Placeholder: Screenshots of the UI would be beneficial here, e.g., Job List, Job Detail with Audit Tab, Job Edit Form.)*

## 8. Database Setup

### 8.1. Oracle Database
- **`JOB_CONFIG` Table**: Stores the master definitions for all ETL jobs.
  - DDL: `ddl/oracle_job_config_ddl.sql`
  - Detailed Schema: `documentation/oracle_config_schema.md`
- **`JOB_CONFIG_AUDIT_LOG` Table**: Logs changes made to `JOB_CONFIG` entries via the API.
  - DDL: `ddl/oracle_job_config_audit_log_ddl.sql`
  - Detailed Schema: `documentation/oracle_config_schema.md`

### 8.2. Apache Hive (for Spark Application Auditing)
- The Spark application logs job run summaries, detailed run information, and data rejection events to Hive tables.
- An audit database (e.g., `etl_framework_audit`) must be created in Hive by an administrator.
- **Table DDLs**:
  - `ddl/hive_audit_summary_table.hql`
  - `ddl/hive_job_run_details_table.hql`
  - `ddl/hive_rejection_events_table.hql`
- These tables are created as EXTERNAL tables with example HDFS locations. Adjust locations as per your environment.

## 9. Key Configuration Files & Concepts

- **`JOB_CONFIG` Table (Oracle)**: This is the central piece driving the entire framework. Understanding its columns and how they interact is crucial. Refer to `documentation/oracle_config_schema.md` and `documentation/job_configuration_examples.md`.
- **`job_properties` field in `JOB_CONFIG`**: A JSON CLOB for extensible, non-standard configurations (e.g., `schema_drift_new_source_columns_behavior`, `scd2_handle_deletes_by_absence`).
- **Runtime Spark Configurations**: Passed via `spark-submit --conf`, used for database connections, SMTP settings, and dynamic parameters for SQL logic (e.g., `spark.etl.param.processing_date`).

## 10. How to Contribute
*(Placeholder: Contribution guidelines, code style, pull request process, etc.)*

## 11. License
*(Placeholder: e.g., MIT License, Apache 2.0 License. Specify the chosen license here.)*
