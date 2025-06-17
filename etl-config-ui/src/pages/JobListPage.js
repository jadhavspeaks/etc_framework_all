import React, { useState, useEffect } from 'react';
import { fetchJobConfigs, setAuthCredentials } from '../services/jobConfigApiService';
import {
    Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper,
    TablePagination, CircularProgress, Typography, TextField, Button, Box, Alert
} from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';

// Temporary: Set dummy credentials for testing. Remove in real app.
// In a real app, this would come from a login form/context.
setAuthCredentials('viewer', 'viewerpass');

const JobListPage = () => {
    const [jobsPage, setJobsPage] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [page, setPage] = useState(0);
    const [rowsPerPage, setRowsPerPage] = useState(10);
    const [jobNameFilter, setJobNameFilter] = useState('');
    const [sourceTypeFilter, setSourceTypeFilter] = useState('');

    const loadJobs = () => {
        setLoading(true);
        setError('');
        fetchJobConfigs(page, rowsPerPage, 'jobName,asc', jobNameFilter, sourceTypeFilter)
            .then(data => {
                setJobsPage(data);
                setLoading(false);
            })
            .catch(err => {
                console.error("API Error:", err);
                setError('Failed to fetch job configurations. ' + (err.response?.data?.message || err.message));
                setLoading(false);
            });
    };

    useEffect(() => {
        loadJobs();
    }, [page, rowsPerPage]); // Reload when page or rowsPerPage changes

    const handleFilterSubmit = (event) => {
        event.preventDefault();
        setPage(0); // Reset to first page on new filter
        loadJobs();
    };

    const handleChangePage = (event, newPage) => {
        setPage(newPage);
    };

    const handleChangeRowsPerPage = (event) => {
        setRowsPerPage(parseInt(event.target.value, 10));
        setPage(0);
    };

    if (loading) return <CircularProgress />;
    if (error) return <Alert severity="error">{error}</Alert>;
    if (!jobsPage || !jobsPage.content) return <Typography>No job configurations found.</Typography>;

    return (
        <Paper sx={{ width: '100%', overflow: 'hidden', padding: 2 }}>
            <Typography variant="h5" gutterBottom>Job Configurations</Typography>
            <Box component="form" onSubmit={handleFilterSubmit} sx={{ mb: 2, display: 'flex', gap: 2 }}>
                <TextField
                    label="Filter by Job Name"
                    variant="outlined"
                    size="small"
                    value={jobNameFilter}
                    onChange={(e) => setJobNameFilter(e.target.value)}
                />
                <TextField
                    label="Filter by Source Type"
                    variant="outlined"
                    size="small"
                    value={sourceTypeFilter}
                    onChange={(e) => setSourceTypeFilter(e.target.value)}
                />
                <Button type="submit" variant="contained">Filter</Button>
            </Box>
            <TableContainer>
                <Table stickyHeader aria-label="job configs table">
                    <TableHead>
                        <TableRow>
                            <TableCell>Job Name</TableCell>
                            <TableCell>Description</TableCell>
                            <TableCell>Source Type</TableCell>
                            <TableCell>Target Type</TableCell>
                            <TableCell>Transform Mode</TableCell>
                            <TableCell>Active</TableCell>
                            <TableCell>Last Updated</TableCell>
                            <TableCell>Actions</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {jobsPage.content.map((job) => (
                            <TableRow hover key={job.jobName}>
                                <TableCell>{job.jobName}</TableCell>
                                <TableCell>{job.jobDescription}</TableCell>
                                <TableCell>{job.sourceType}</TableCell>
                                <TableCell>{job.targetType}</TableCell>
                                <TableCell>{job.transformationMode}</TableCell>
                                <TableCell>{job.isActive}</TableCell>
                                <TableCell>{job.updatedTs ? new Date(job.updatedTs).toLocaleString() : 'N/A'}</TableCell>
                                <TableCell>
                                    <Button component={RouterLink} to={`/jobs/${job.jobName}`} size="small" sx={{mr:1}}>View</Button>
                                    <Button component={RouterLink} to={`/jobs/${job.jobName}/edit`} size="small" color="secondary">Edit</Button>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
            <TablePagination
                rowsPerPageOptions={[5, 10, 25]}
                component="div"
                count={jobsPage.totalElements}
                rowsPerPage={rowsPerPage}
                page={jobsPage.number} // Spring Page is 0-indexed
                onPageChange={handleChangePage}
                onRowsPerPageChange={handleChangeRowsPerPage}
            />
        </Paper>
    );
};

export default JobListPage;
