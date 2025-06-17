import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import JobForm from '../components/JobForm';
import { createJobConfig } from '../services/jobConfigApiService';
import { Typography, Container, Alert } from '@mui/material';

const CreateJobPage = () => {
    const navigate = useNavigate();
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);

    const handleSubmit = async (formData) => {
        setError(null);
        setSuccess(null);
        // Convert numeric-like strings to numbers before submitting if necessary
        // JobForm's handleSubmit already does this, so formData should be good.
        const dataToSubmit = formData;

        try {
            console.log("Form Data to Submit for Create:", dataToSubmit);
            const createdJob = await createJobConfig(dataToSubmit);

            setSuccess(`Job '${createdJob.jobName}' created successfully! Redirecting...`);
            setTimeout(() => {
                // Navigate to the job detail page or job list
                navigate(`/jobs/${createdJob.jobName}`); // Or navigate('/jobs');
            }, 2000);

        } catch (err) {
            console.error("Create Job Error:", err);
            if (err.response) {
                if (err.response.data && err.response.data.errors && Array.isArray(err.response.data.errors)) {
                    setError(err.response.data.errors.map(e => e.defaultMessage || e).join(', '));
                } else if (err.response.data && typeof err.response.data === 'string' && err.response.data.includes("Job configuration with name")) { // Specific for conflict from service
                    setError(err.response.data);
                } else if (err.response.data && err.response.data.message) {
                    setError(err.response.data.message);
                } else if (err.response.status === 409) {
                    setError('Conflict: A job with this name already exists.');
                } else if (err.response.status === 400 && Array.isArray(err.response.data)) { // From /validate endpoint style
                    setError("Validation Failed: " + err.response.data.join('; '));
                } else {
                    setError(`Error creating job (Status: ${err.response.status}): ${err.message}`);
                }
            } else {
                setError(`Error creating job: ${err.message}`);
            }
        }
    };

    const handleCancel = () => {
        navigate('/jobs');
    };

    return (
        <Container maxWidth="lg">
            {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
            {success && <Alert severity="success" sx={{ mb: 2 }}>{success}</Alert>}
            <JobForm
                onSubmit={handleSubmit}
                onCancel={handleCancel}
                isEditMode={false}
            />
        </Container>
    );
};

export default CreateJobPage;
