import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import JobForm from '../components/JobForm';
import { fetchJobConfigByName, updateJobConfig } from '../services/jobConfigApiService';
import { Typography, Container, Alert, CircularProgress, Box } from '@mui/material';

const EditJobPage = () => {
    const { jobName } = useParams();
    const navigate = useNavigate();
    const [initialData, setInitialData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);

    useEffect(() => {
        if (jobName) {
            setLoading(true);
            setError(null);
            fetchJobConfigByName(jobName)
                .then(data => {
                    setInitialData(data);
                    setLoading(false);
                })
                .catch(err => {
                    console.error("Fetch Job Error:", err);
                    setError(`Failed to fetch job '${jobName}'. ` + (err.response?.data?.message || err.message));
                    setLoading(false);
                });
        }
    }, [jobName]);

    const handleSubmit = async (formData) => {
        setError(null);
        setSuccess(null);
        // JobForm's handleSubmit already prepares numeric types.
        const dataToSubmit = formData;

        try {
            console.log("Form Data to Update for", jobName, ":", dataToSubmit);
            const updatedJob = await updateJobConfig(jobName, dataToSubmit);

            setSuccess(`Job '${updatedJob.jobName}' updated successfully! Redirecting...`);
            setTimeout(() => {
                navigate(`/jobs/${updatedJob.jobName}`);
            }, 1500);

        } catch (err) {
            console.error("Update Job Error:", err);
            if (err.response) {
                if (err.response.data && err.response.data.errors && Array.isArray(err.response.data.errors)) {
                    setError(err.response.data.errors.map(e => e.defaultMessage || e).join(', '));
                } else if (err.response.data && err.response.data.message) {
                    setError(err.response.data.message);
                } else if (err.response.status === 400 && Array.isArray(err.response.data)) { // From /validate endpoint style
                    setError("Validation Failed: " + err.response.data.join('; '));
                } else {
                    setError(`Error updating job (Status: ${err.response.status}): ${err.message}`);
                }
            } else {
                setError(`Error updating job: ${err.message}`);
            }
        }
    };

    const handleCancel = () => {
        navigate(initialData ? `/jobs/${jobName}` : '/jobs');
    };

    if (loading) return <CircularProgress />;
    if (error && !initialData && !loading) return <Alert severity="error" sx={{ m: 2 }}>{error}</Alert>;

    return (
        <Container maxWidth="lg">
            {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
            {success && <Alert severity="success" sx={{ mb: 2 }}>{success}</Alert>}
            {initialData ? (
                <JobForm
                    initialData={initialData}
                    onSubmit={handleSubmit}
                    onCancel={handleCancel}
                    isEditMode={true}
                />
            ) : (
                !loading && <Typography>Job data could not be loaded for editing or does not exist.</Typography>
            )}
        </Container>
    );
};

export default EditJobPage;
