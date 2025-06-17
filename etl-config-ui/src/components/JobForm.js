import React, { useState, useEffect } from 'react';
import {
    TextField, Button, Grid, Paper, Typography, Box,
    FormControl, InputLabel, Select, MenuItem, Checkbox, FormControlLabel
} from '@mui/material';

const JobForm = ({ initialData, onSubmit, onCancel, isEditMode = false }) => {
    const [formData, setFormData] = useState({});

    useEffect(() => {
        // Initialize form with initialData or defaults
        const defaults = {
            jobName: '',
            jobDescription: '',
            isActive: 'Y',
            sourceType: 'FILE',
            sourceConnectionDetails: '',
            sourceFormat: 'PARQUET',
            sourceFormatOptions: '{}',
            sourceSchema: '',
            targetType: 'HIVE_TABLE',
            targetConnectionDetails: '',
            targetFormat: 'PARQUET',
            targetFormatOptions: '{}',
            targetTableOrPath: '',
            loadType: 'INCREMENTAL',
            targetWriteMode: 'overwrite',
            transformationMode: 'AS_IS',
            sqlLogic: '',
            schemaMappingLogic: '{}',
            // SCD2 fields - keep them simple for now
            scd2NaturalKeys: '',
            scd2ChangeTrackingColumn: '',
            scd2SurrogateKeyColumn: 'sk_id',
            scd2ValidFromColumn: 'valid_from_ts',
            scd2ValidToColumn: 'valid_to_ts',
            scd2VersionColumn: 'version',
            scd2CurrentFlagColumn: 'is_current',
            // Other fields
            partitioningColumns: '',
            jobPriority: 0,
            maxRetries: 3,
            retryBackoffMs: 60000,
            slaThresholdMinutes: '', // Keep as string for TextField, convert on submit
            dependencyJobIds: '',
            auditLevel: 'JOB',
            reconciliationEnabled: 'N',
            reconciliationConfig: '{}',
            dqChecksEnabled: 'N',
            dqRulesConfig: '[]',
            notificationEmailsSuccess: '',
            notificationEmailsFailure: '',
            notificationVerbosity: 'BRIEF',
            jobProperties: '{}',
            logMaskingColumns: '',
            executionAuthorizationFlag: 'Y',
            manualTriggerOnly: 'N',
        };
        setFormData(initialData ? { ...defaults, ...initialData } : defaults);
    }, [initialData]);

    const handleChange = (event) => {
        const { name, value, type, checked } = event.target;
        setFormData(prev => ({
            ...prev,
            [name]: type === 'checkbox' ? (checked ? 'Y' : 'N') : value
        }));
    };

    const handleNumericChange = (event) => {
        const { name, value } = event.target;
        // Allow empty string for optional numerics, or actual numbers
        if (value === '' || /^[0-9]+$/.test(value)) {
            setFormData(prev => ({ ...prev, [name]: value }));
        }
    };

    const handleSubmit = (event) => {
        event.preventDefault();
        // Convert numeric-like strings to numbers before submitting if necessary
        const dataToSubmit = {
            ...formData,
            jobPriority: parseInt(formData.jobPriority, 10) || 0,
            maxRetries: parseInt(formData.maxRetries, 10) || 0,
            retryBackoffMs: parseInt(formData.retryBackoffMs, 10) || 0,
            slaThresholdMinutes: formData.slaThresholdMinutes ? parseInt(formData.slaThresholdMinutes, 10) : null,
        };
        onSubmit(dataToSubmit);
    };

    // For brevity, only a subset of fields are rendered here.
    // In a full implementation, all fields from JobConfigCreateDTO/UpdateDTO would be rendered.
    return (
        <Paper elevation={3} sx={{ p: 3 }}>
            <Typography variant="h5" gutterBottom>
                {isEditMode ? `Edit Job: ${initialData?.jobName}` : 'Create New ETL Job'}
            </Typography>
            <Box component="form" onSubmit={handleSubmit} noValidate sx={{ mt: 1 }}>
                <Grid container spacing={2}>
                    <Grid item xs={12} sm={6}>
                        <TextField
                            required
                            fullWidth
                            id="jobName"
                            label="Job Name"
                            name="jobName"
                            value={formData.jobName || ''}
                            onChange={handleChange}
                            disabled={isEditMode} // Job name usually not editable
                        />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                        <FormControl fullWidth>
                            <InputLabel id="is-active-label">Is Active</InputLabel>
                            <Select
                                labelId="is-active-label"
                                id="isActive"
                                name="isActive"
                                value={formData.isActive || 'Y'}
                                label="Is Active"
                                onChange={handleChange}
                            >
                                <MenuItem value="Y">Yes</MenuItem>
                                <MenuItem value="N">No</MenuItem>
                            </Select>
                        </FormControl>
                    </Grid>
                    <Grid item xs={12}>
                        <TextField
                            fullWidth
                            id="jobDescription"
                            label="Job Description"
                            name="jobDescription"
                            multiline
                            rows={3}
                            value={formData.jobDescription || ''}
                            onChange={handleChange}
                        />
                    </Grid>

                    {/* --- Source Configuration --- */}
                    <Grid item xs={12}><Typography variant="h6" sx={{mt:2}}>Source Configuration</Typography></Grid>
                    <Grid item xs={12} sm={6}>
                         <FormControl fullWidth>
                            <InputLabel id="source-type-label">Source Type</InputLabel>
                            <Select labelId="source-type-label" name="sourceType" value={formData.sourceType || 'FILE'} label="Source Type" onChange={handleChange}>
                                <MenuItem value="FILE">FILE</MenuItem>
                                <MenuItem value="HIVE_TABLE">HIVE_TABLE</MenuItem>
                                {/* Add KAFKA, JDBC later */}
                            </Select>
                        </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={6}>
                        <TextField fullWidth label="Source Connection Details" name="sourceConnectionDetails" value={formData.sourceConnectionDetails || ''} onChange={handleChange} helperText={formData.sourceType === 'FILE' ? 'Path to file/dir' : (formData.sourceType === 'HIVE_TABLE' ? 'db.tableName' : '')}/>
                    </Grid>
                    {/* Conditional fields for FILE source type - basic example */}
                    {formData.sourceType === 'FILE' && (
                        <>
                            <Grid item xs={12} sm={6}><TextField fullWidth label="Source Format (e.g., PARQUET)" name="sourceFormat" value={formData.sourceFormat || ''} onChange={handleChange} /></Grid>
                            <Grid item xs={12} sm={6}><TextField fullWidth label="Source Format Options (JSON)" name="sourceFormatOptions" value={formData.sourceFormatOptions || ''} onChange={handleChange} /></Grid>
                            <Grid item xs={12}><TextField fullWidth label="Source Schema (DDL or JSON)" name="sourceSchema" multiline rows={2} value={formData.sourceSchema || ''} onChange={handleChange} /></Grid>
                        </>
                    )}

                    {/* --- Target Configuration --- */}
                    <Grid item xs={12}><Typography variant="h6" sx={{mt:2}}>Target Configuration</Typography></Grid>
                     <Grid item xs={12} sm={6}>
                         <FormControl fullWidth>
                            <InputLabel id="target-type-label">Target Type</InputLabel>
                            <Select labelId="target-type-label" name="targetType" value={formData.targetType || 'HIVE_TABLE'} label="Target Type" onChange={handleChange}>
                                <MenuItem value="FILE">FILE</MenuItem>
                                <MenuItem value="HDFS">HDFS</MenuItem> // HDFS is essentially FILE path
                                <MenuItem value="HIVE_TABLE">HIVE_TABLE</MenuItem>
                                {/* Add KAFKA, ORACLE_TABLE later */}
                            </Select>
                        </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={6}>
                        <TextField fullWidth label="Target Table or Path" name="targetTableOrPath" required value={formData.targetTableOrPath || ''} onChange={handleChange} />
                    </Grid>
                    {/* Conditional fields for FILE/HDFS target type - basic example */}
                    {(formData.targetType === 'FILE' || formData.targetType === 'HDFS') && (
                        <>
                            <Grid item xs={12} sm={6}><TextField fullWidth label="Target Format (e.g., PARQUET)" name="targetFormat" value={formData.targetFormat || ''} onChange={handleChange} /></Grid>
                            <Grid item xs={12} sm={6}><TextField fullWidth label="Target Format Options (JSON)" name="targetFormatOptions" value={formData.targetFormatOptions || ''} onChange={handleChange} /></Grid>
                        </>
                    )}

                    {/* --- Transformation and Load --- */}
                    <Grid item xs={12}><Typography variant="h6" sx={{mt:2}}>Transformation & Load</Typography></Grid>
                    <Grid item xs={12} sm={4}>
                        <FormControl fullWidth>
                            <InputLabel id="transformation-mode-label">Transformation Mode</InputLabel>
                            <Select labelId="transformation-mode-label" name="transformationMode" value={formData.transformationMode || 'AS_IS'} label="Transformation Mode" onChange={handleChange}>
                                <MenuItem value="AS_IS">AS_IS</MenuItem>
                                <MenuItem value="WITH_LOGIC">WITH_LOGIC</MenuItem>
                                <MenuItem value="SCD2">SCD2</MenuItem>
                            </Select>
                        </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={4}>
                        <FormControl fullWidth>
                            <InputLabel id="load-type-label">Load Type</InputLabel>
                            <Select labelId="load-type-label" name="loadType" value={formData.loadType || 'INCREMENTAL'} label="Load Type" onChange={handleChange}>
                                <MenuItem value="INCREMENTAL">INCREMENTAL</MenuItem>
                                <MenuItem value="FULL_RELOAD">FULL_RELOAD</MenuItem>
                            </Select>
                        </FormControl>
                    </Grid>
                    <Grid item xs={12} sm={4}>
                        <FormControl fullWidth>
                            <InputLabel id="write-mode-label">Target Write Mode</InputLabel>
                            <Select labelId="write-mode-label" name="targetWriteMode" value={formData.targetWriteMode || 'overwrite'} label="Target Write Mode" onChange={handleChange}>
                                <MenuItem value="overwrite">Overwrite</MenuItem>
                                <MenuItem value="append">Append</MenuItem>
                                <MenuItem value="ignore">Ignore</MenuItem>
                                <MenuItem value="errorifexists">ErrorIfExists</MenuItem>
                            </Select>
                        </FormControl>
                    </Grid>

                    {/* Conditional SQL Logic field */}
                    {(formData.transformationMode === 'WITH_LOGIC' || formData.transformationMode === 'SCD2') && (
                        <Grid item xs={12}>
                            <TextField
                                fullWidth
                                label="SQL Logic"
                                name="sqlLogic"
                                multiline rows={4}
                                value={formData.sqlLogic || ''}
                                onChange={handleChange}
                                required={formData.transformationMode === 'WITH_LOGIC' || formData.transformationMode === 'SCD2'}
                            />
                        </Grid>
                    )}

                    {/* More fields for schema_mapping_logic, SCD2 params, execution control, audit, recon, DQ, notifications, job_properties etc. would go here */}
                    {/* For brevity, these are not all explicitly laid out in this initial form structure subtask */}

                    <Grid item xs={12} sx={{mt: 2}}>
                        <Button type="submit" variant="contained" color="primary" sx={{ mr: 1 }}>
                            {isEditMode ? 'Save Changes' : 'Create Job'}
                        </Button>
                        <Button variant="outlined" onClick={onCancel}>
                            Cancel
                        </Button>
                    </Grid>
                </Grid>
            </Box>
        </Paper>
    );
};

export default JobForm;
