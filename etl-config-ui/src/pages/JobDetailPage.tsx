import React, { useEffect, useState } from 'react';
import { useParams, Link as RouterLink } from 'react-router-dom';
import { Box, Typography, CircularProgress, Alert, Card, CardContent, Grid, Button, Divider, Chip, Container, Tabs, Tab } from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import EditIcon from '@mui/icons-material/Edit';
import apiClient from '../services/apiClient';
import AuditLogTabContent from '../../components/jobs/AuditLogTabContent';
import { useAuth } from '../../context/AuthContext'; // Import useAuth

interface JobConfigDetail { jobName: string; jobDescription?: string; isActive: string; sourceType: string; sourceConnectionDetails?: string; sourceFormat?: string; sourceFormatOptions?: string; sourceSchema?: string; targetType: string; targetConnectionDetails?: string; targetFormat?: string; targetFormatOptions?: string; targetTableOrPath: string; loadType: string; targetWriteMode: string; transformationMode: string; sqlLogic?: string; schemaMappingLogic?: string; scd2NaturalKeys?: string; scd2ChangeTrackingColumn?: string; scd2SurrogateKeyColumn?: string; scd2ValidFromColumn?: string; scd2ValidToColumn?: string; scd2VersionColumn?: string; scd2CurrentFlagColumn?: string; partitioningColumns?: string; jobPriority?: number; maxRetries?: number; retryBackoffMs?: number; slaThresholdMinutes?: number; dependencyJobIds?: string; auditLevel: string; reconciliationEnabled: string; reconciliationConfig?: string; dqChecksEnabled: string; dqRulesConfig?: string; notificationEmailsSuccess?: string; notificationEmailsFailure?: string; notificationVerbosity?: string; jobProperties?: string; logMaskingColumns?: string; executionAuthorizationFlag: string; manualTriggerOnly: string; createdBy: string; createdTs: string; updatedBy: string; updatedTs: string; configVersion: number; }

const DetailItem: React.FC<{ label: string; value?: string | number | null }> = ({ label, value }) => (
    <Grid item xs={12} sm={6} md={4}>
        <Typography variant="subtitle2" color="textSecondary" gutterBottom>{label}:</Typography>
        <Typography variant="body1" sx={{ wordBreak: 'break-word' }}>{value != null ? String(value) : <Chip label="Not Set" size="small" />}</Typography>
    </Grid>
);

const CLOBDataView: React.FC<{ label: string; data?: string | null, isJson?: boolean }> = ({ label, data, isJson = false }) => {
    let displayData = data;
    if (isJson && data) {
        try {
            const parsedJson = JSON.parse(data);
            displayData = JSON.stringify(parsedJson, null, 2);
        } catch (e) {
            console.error("Failed to parse JSON for display:", e);
        }
    }
    return (
        <Grid item xs={12}>
            <Typography variant="subtitle2" color="textSecondary" gutterBottom>{label}:</Typography>
            {data ? (
                <Box component="pre" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all', backgroundColor: '#f5f5f5', padding: 1, borderRadius: 1, maxHeight: 300, overflowY: 'auto' }}>
                    {displayData}
                </Box>
            ) : <Chip label="Not Set" size="small" />}
        </Grid>
    );
};

const JobDetailPage: React.FC = () => {
    const { jobName } = useParams<{ jobName: string }>();
    const auth = useAuth(); // Get auth context
    const [jobDetail, setJobDetail] = useState<JobConfigDetail | null>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);
    const [currentTab, setCurrentTab] = useState(0);

    const handleTabChange = (event: React.SyntheticEvent, newValue: number) => { setCurrentTab(newValue); };

    useEffect(() => {
        if (!jobName) return;
        setLoading(true); setError(null);
        apiClient.get<JobConfigDetail>(`/jobconfigs/${jobName}`)
            .then(response => setJobDetail(response.data))
            .catch(err => setError(err.response?.data?.message || err.message || 'Failed to fetch details.'))
            .finally(() => setLoading(false));
    }, [jobName]);

    const userRoles = auth.user?.roles || [];
    const canEdit = userRoles.includes('ROLE_EDITOR') || userRoles.includes('ROLE_ADMIN');

    if (loading) return <Box sx={{ display: 'flex', justifyContent: 'center', p:3 }}><CircularProgress /></Box>;
    if (error) return <Alert severity="error">{error}</Alert>;
    if (!jobDetail) return <Alert severity="info">No details for {jobName}.</Alert>;

    return (
        <Container maxWidth="lg">
            <Button component={RouterLink} to="/" startIcon={<ArrowBackIcon />} sx={{ mb: 2 }}>Back to List</Button>
            <Card>
                <CardContent>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                        <Typography variant="h4" component="h1">{jobDetail.jobName}</Typography>
                        {canEdit && (
                            <Button component={RouterLink} to={`/jobs/${jobDetail.jobName}/edit`} variant="contained" startIcon={<EditIcon />}>
                                Edit Job
                            </Button>
                        )}
                    </Box>
                    <Typography variant="subtitle1" color="textSecondary" gutterBottom>{jobDetail.jobDescription || 'No description.'}</Typography>
                    <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}><Tabs value={currentTab} onChange={handleTabChange} aria-label="job detail tabs"><Tab label="Details" /><Tab label="Audit Log" /></Tabs></Box>
                    {/* Tab Panel for Configuration Details */}
                    <Box role="tabpanel" hidden={currentTab !== 0} id="job-details-panel-0" aria-labelledby="job-details-tab-0">
                        {currentTab === 0 && (
                            <Box>
                                <Divider sx={{ my: 2 }}><Chip label="Core Configuration" /></Divider>
                                <Grid container spacing={2}>
                                    <DetailItem label="Active" value={jobDetail.isActive} />
                                    <DetailItem label="Transformation Mode" value={jobDetail.transformationMode} />
                                    <DetailItem label="Load Type" value={jobDetail.loadType} />
                                    <DetailItem label="Target Write Mode" value={jobDetail.targetWriteMode} />
                                </Grid>
                                <Divider sx={{ my: 2 }} />

                                <Typography variant="h6" gutterBottom>Source Configuration</Typography>
                                <Grid container spacing={2}>
                                    <DetailItem label="Source Type" value={jobDetail.sourceType} />
                                    <DetailItem label="Connection Details" value={jobDetail.sourceConnectionDetails} />
                                    <DetailItem label="Format" value={jobDetail.sourceFormat} />
                                    <CLOBDataView label="Format Options (JSON)" data={jobDetail.sourceFormatOptions} isJson={true} />
                                    <CLOBDataView label="Schema (CLOB)" data={jobDetail.sourceSchema} />
                                </Grid>
                                <Divider sx={{ my: 2 }} />

                                <Typography variant="h6" gutterBottom>Target Configuration</Typography>
                                <Grid container spacing={2}>
                                    <DetailItem label="Target Type" value={jobDetail.targetType} />
                                    <DetailItem label="Connection Details" value={jobDetail.targetConnectionDetails} />
                                    <DetailItem label="Table/Path" value={jobDetail.targetTableOrPath} />
                                    <DetailItem label="Format" value={jobDetail.targetFormat} />
                                    <CLOBDataView label="Format Options (JSON)" data={jobDetail.targetFormatOptions} isJson={true} />
                                    <DetailItem label="Partitioning Columns" value={jobDetail.partitioningColumns} />
                                </Grid>
                                <Divider sx={{ my: 2 }} />

                                {jobDetail.transformationMode !== 'AS_IS' && (
                                    <>
                                        <Typography variant="h6" gutterBottom>Transformation Logic</Typography>
                                        <Grid container spacing={2}>
                                            <CLOBDataView label="SQL Logic (CLOB)" data={jobDetail.sqlLogic} />
                                            <CLOBDataView label="Schema Mapping Logic (JSON CLOB)" data={jobDetail.schemaMappingLogic} isJson={true} />
                                        </Grid>
                                        <Divider sx={{ my: 2 }} />
                                    </>
                                )}

                                {jobDetail.transformationMode === 'SCD2' && (
                                    <>
                                        <Typography variant="h6" gutterBottom>SCD Type 2 Configuration</Typography>
                                        <Grid container spacing={2}>
                                            <DetailItem label="Natural Keys" value={jobDetail.scd2NaturalKeys} />
                                            <DetailItem label="Change Tracking Column" value={jobDetail.scd2ChangeTrackingColumn} />
                                            <DetailItem label="Surrogate Key Col" value={jobDetail.scd2SurrogateKeyColumn} />
                                            <DetailItem label="Valid From Col" value={jobDetail.scd2ValidFromColumn} />
                                            <DetailItem label="Valid To Col" value={jobDetail.scd2ValidToColumn} />
                                            <DetailItem label="Version Col" value={jobDetail.scd2VersionColumn} />
                                            <DetailItem label="Current Flag Col" value={jobDetail.scd2CurrentFlagColumn} />
                                        </Grid>
                                        <Divider sx={{ my: 2 }} />
                                    </>
                                )}

                                <Typography variant="h6" gutterBottom>Execution & Operational</Typography>
                                <Grid container spacing={2}>
                                    <DetailItem label="Job Priority" value={jobDetail.jobPriority} />
                                    <DetailItem label="Max Retries" value={jobDetail.maxRetries} />
                                    <DetailItem label="Retry Backoff (ms)" value={jobDetail.retryBackoffMs} />
                                    <DetailItem label="SLA Threshold (mins)" value={jobDetail.slaThresholdMinutes} />
                                    <DetailItem label="Dependency Job Names" value={jobDetail.dependencyJobIds} />
                                    <DetailItem label="Audit Level" value={jobDetail.auditLevel} />
                                    <DetailItem label="Reconciliation Enabled" value={jobDetail.reconciliationEnabled} />
                                    <CLOBDataView label="Reconciliation Config (JSON CLOB)" data={jobDetail.reconciliationConfig} isJson={true} />
                                    <DetailItem label="DQ Checks Enabled" value={jobDetail.dqChecksEnabled} />
                                    <CLOBDataView label="DQ Rules Config (JSON CLOB)" data={jobDetail.dqRulesConfig} isJson={true} />
                                    <DetailItem label="Notification Emails (Success)" value={jobDetail.notificationEmailsSuccess} />
                                    <DetailItem label="Notification Emails (Failure)" value={jobDetail.notificationEmailsFailure} />
                                    <DetailItem label="Notification Verbosity" value={jobDetail.notificationVerbosity} />
                                    <CLOBDataView label="Job Properties (JSON CLOB)" data={jobDetail.jobProperties} isJson={true} />
                                    <DetailItem label="Log Masking Columns" value={jobDetail.logMaskingColumns} />
                                    <DetailItem label="Execution Auth Flag" value={jobDetail.executionAuthorizationFlag} />
                                    <DetailItem label="Manual Trigger Only" value={jobDetail.manualTriggerOnly} />
                                </Grid>
                                <Divider sx={{ my: 2 }} />

                                <Typography variant="h6" gutterBottom>Metadata</Typography>
                                <Grid container spacing={2}>
                                    <DetailItem label="Created By" value={jobDetail.createdBy} />
                                    <DetailItem label="Created Timestamp" value={new Date(jobDetail.createdTs).toLocaleString()} />
                                    <DetailItem label="Updated By" value={jobDetail.updatedBy} />
                                    <DetailItem label="Updated Timestamp" value={new Date(jobDetail.updatedTs).toLocaleString()} />
                                    <DetailItem label="Config Version" value={jobDetail.configVersion} />
                                </Grid>
                            </Box>
                        )}
                    </Box>

                    <Box role="tabpanel" hidden={currentTab !== 1} id="job-details-panel-1" aria-labelledby="job-details-tab-1">
                        {currentTab === 1 && jobName && (
                           <AuditLogTabContent jobName={jobName} />
                        )}
                    </Box>

                </CardContent>
            </Card>
        </Container>
    );
};

export default JobDetailPage;
