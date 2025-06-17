import React, { useState, useEffect } from 'react';
import { useParams, Link as RouterLink } from 'react-router-dom';
import { fetchJobConfigByName } from '../services/jobConfigApiService';
import {
    CircularProgress, Typography, Paper, List, ListItem, ListItemText,
    Divider, Grid, Box, Button, Alert
} from '@mui/material';

const DetailItem = ({ primary, secondary }) => (
    <ListItem>
        <ListItemText
            primary={primary}
            secondary={secondary || "N/A"}
            primaryTypographyProps={{ fontWeight: 'bold' }}
        />
    </ListItem>
);

const JobDetailPage = () => {
    const { jobName } = useParams(); // Get jobName from URL parameter
    const [jobConfig, setJobConfig] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');

    useEffect(() => {
        if (jobName) {
            setLoading(true);
            setError('');
            fetchJobConfigByName(jobName)
                .then(data => {
                    setJobConfig(data);
                    setLoading(false);
                })
                .catch(err => {
                    console.error("API Error:", err);
                    setError(`Failed to fetch job configuration for '${jobName}'. ` + (err.response?.data?.message || err.message));
                    setLoading(false);
                });
        }
    }, [jobName]);

    if (loading) return <CircularProgress />;
    if (error) return <Alert severity="error">{error}</Alert>;
    if (!jobConfig) return <Typography>Job configuration not found.</Typography>;

    // Helper to render CLOB/JSON string content
    const renderJsonString = (jsonString) => {
        if (!jsonString || jsonString.trim() === '') return <Typography variant="body2">N/A</Typography>;
        try {
            const parsed = JSON.parse(jsonString);
            return <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all', backgroundColor: '#f5f5f5', padding: '10px', borderRadius: '4px' }}>{JSON.stringify(parsed, null, 2)}</pre>;
        } catch (e) {
            // If not valid JSON, display as is (might be simple string or malformed)
            return <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{jsonString}</Typography>;
        }
    };

    return (
        <Paper sx={{ padding: 3, margin: 2 }}>
            <Typography variant="h4" gutterBottom>Job: {jobConfig.jobName}</Typography>
            <Button component={RouterLink} to="/jobs" variant="outlined" sx={{ mb: 2 }}>Back to List</Button>

            <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                    <Typography variant="h6">Core Details</Typography>
                    <List dense>
                        <DetailItem primary="Job Description" secondary={jobConfig.jobDescription} />
                        <DetailItem primary="Is Active" secondary={jobConfig.isActive} />
                        <DetailItem primary="Transformation Mode" secondary={jobConfig.transformationMode} />
                        <DetailItem primary="Load Type" secondary={jobConfig.loadType} />
                        <DetailItem primary="Target Write Mode" secondary={jobConfig.targetWriteMode} />
                    </List>
                </Grid>
                <Grid item xs={12} md={6}>
                    <Typography variant="h6">Execution Control</Typography>
                    <List dense>
                        <DetailItem primary="Job Priority" secondary={jobConfig.jobPriority?.toString()} />
                        <DetailItem primary="Max Retries" secondary={jobConfig.maxRetries?.toString()} />
                        <DetailItem primary="Retry Backoff (ms)" secondary={jobConfig.retryBackoffMs?.toString()} />
                        <DetailItem primary="SLA Threshold (min)" secondary={jobConfig.slaThresholdMinutes?.toString()} />
                        <DetailItem primary="Dependency Job Names" secondary={jobConfig.dependencyJobIds} />
                    </List>
                </Grid>

                <Grid item xs={12}><Divider sx={{ my: 1 }} /></Grid>

                <Grid item xs={12} md={6}>
                    <Typography variant="h6">Source Configuration</Typography>
                    <List dense>
                        <DetailItem primary="Source Type" secondary={jobConfig.sourceType} />
                        <DetailItem primary="Connection Details" secondary={jobConfig.sourceConnectionDetails} />
                        <DetailItem primary="Format" secondary={jobConfig.sourceFormat} />
                    </List>
                </Grid>
                <Grid item xs={12} md={6}>
                    <Typography variant="h6">Target Configuration</Typography>
                    <List dense>
                        <DetailItem primary="Target Type" secondary={jobConfig.targetType} />
                        <DetailItem primary="Connection Details" secondary={jobConfig.targetConnectionDetails} />
                        <DetailItem primary="Format" secondary={jobConfig.targetFormat} />
                        <DetailItem primary="Table/Path" secondary={jobConfig.targetTableOrPath} />
                    </List>
                </Grid>

                <Grid item xs={12}><Divider sx={{ my: 1 }} /></Grid>

                <Grid item xs={12} md={6}>
                    <Typography variant="h6">SCD2 Configuration</Typography>
                    <List dense>
                        <DetailItem primary="Natural Keys" secondary={jobConfig.scd2NaturalKeys} />
                        <DetailItem primary="Change Tracking Column" secondary={jobConfig.scd2ChangeTrackingColumn} />
                        <DetailItem primary="SK Column" secondary={jobConfig.scd2SurrogateKeyColumn} />
                        <DetailItem primary="Valid From Column" secondary={jobConfig.scd2ValidFromColumn} />
                        <DetailItem primary="Valid To Column" secondary={jobConfig.scd2ValidToColumn} />
                        <DetailItem primary="Version Column" secondary={jobConfig.scd2VersionColumn} />
                        <DetailItem primary="Current Flag Column" secondary={jobConfig.scd2CurrentFlagColumn} />
                    </List>
                </Grid>
                 <Grid item xs={12} md={6}>
                    <Typography variant="h6">Operational Settings</Typography>
                    <List dense>
                        <DetailItem primary="Audit Level" secondary={jobConfig.auditLevel} />
                        <DetailItem primary="Reconciliation Enabled" secondary={jobConfig.reconciliationEnabled} />
                        <DetailItem primary="DQ Checks Enabled" secondary={jobConfig.dqChecksEnabled} />
                        <DetailItem primary="Notification Emails (Success)" secondary={jobConfig.notificationEmailsSuccess} />
                        <DetailItem primary="Notification Emails (Failure)" secondary={jobConfig.notificationEmailsFailure} />
                        <DetailItem primary="Notification Verbosity" secondary={jobConfig.notificationVerbosity} />
                        <DetailItem primary="Log Masking Columns" secondary={jobConfig.logMaskingColumns} />
                        <DetailItem primary="Execution Auth Flag" secondary={jobConfig.executionAuthorizationFlag} />
                        <DetailItem primary="Manual Trigger Only" secondary={jobConfig.manualTriggerOnly} />
                    </List>
                </Grid>

                <Grid item xs={12}><Divider sx={{ my: 1 }} /></Grid>
                <Grid item xs={12}><Typography variant="h6">SQL Logic</Typography>{renderJsonString(jobConfig.sqlLogic)}</Grid>
                <Grid item xs={12}><Typography variant="h6">Schema Mapping Logic</Typography>{renderJsonString(jobConfig.schemaMappingLogic)}</Grid>
                <Grid item xs={12}><Typography variant="h6">Source Format Options</Typography>{renderJsonString(jobConfig.sourceFormatOptions)}</Grid>
                <Grid item xs={12}><Typography variant="h6">Target Format Options</Typography>{renderJsonString(jobConfig.targetFormatOptions)}</Grid>
                <Grid item xs={12}><Typography variant="h6">Reconciliation Config</Typography>{renderJsonString(jobConfig.reconciliationConfig)}</Grid>
                <Grid item xs={12}><Typography variant="h6">DQ Rules Config</Typography>{renderJsonString(jobConfig.dqRulesConfig)}</Grid>
                <Grid item xs={12}><Typography variant="h6">Job Properties</Typography>{renderJsonString(jobConfig.jobProperties)}</Grid>
                <Grid item xs={12}><Typography variant="h6">Source Schema</Typography>{renderJsonString(jobConfig.sourceSchema)}</Grid>

                <Grid item xs={12}><Divider sx={{ my: 1 }} /></Grid>
                <Grid item xs={12} md={6}>
                    <Typography variant="h6">Metadata</Typography>
                    <List dense>
                        <DetailItem primary="Created By" secondary={jobConfig.createdBy} />
                        <DetailItem primary="Created Timestamp" secondary={jobConfig.createdTs ? new Date(jobConfig.createdTs).toLocaleString() : 'N/A'} />
                        <DetailItem primary="Updated By" secondary={jobConfig.updatedBy} />
                        <DetailItem primary="Updated Timestamp" secondary={jobConfig.updatedTs ? new Date(jobConfig.updatedTs).toLocaleString() : 'N/A'} />
                        <DetailItem primary="Config Version" secondary={jobConfig.configVersion?.toString()} />
                    </List>
                </Grid>
            </Grid>
        </Paper>
    );
};

export default JobDetailPage;
