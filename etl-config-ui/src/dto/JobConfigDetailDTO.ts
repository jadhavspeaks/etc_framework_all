// This is a simplified version for the frontend.
// Ensure it matches the structure of JobConfigDetailDTO from your backend API responses.
export interface JobConfigDetailDTO {
    jobName: string;
    jobDescription?: string | null;
    isActive: string; // 'Y' or 'N'
    sourceType: string;
    sourceConnectionDetails?: string | null;
    sourceFormat?: string | null;
    sourceFormatOptions?: string | null;
    sourceSchema?: string | null;
    targetType: string;
    targetConnectionDetails?: string | null;
    targetFormat?: string | null;
    targetFormatOptions?: string | null;
    targetTableOrPath: string;
    loadType: string;
    targetWriteMode: string;
    transformationMode: string;
    sqlLogic?: string | null;
    schemaMappingLogic?: string | null;
    // Add other fields as needed by the form or detail views
    scd2NaturalKeys?: string | null;
    scd2ChangeTrackingColumn?: string | null;
    // ... rest of the fields from backend DTO
    jobPriority?: number | null;
    maxRetries?: number | null;
    // ... etc.
}
