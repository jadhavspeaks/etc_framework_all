import React, { useEffect, useState } from 'react';
import { useParams, useNavigate, Link as RouterLink } from 'react-router-dom';
import { Container, Typography, Alert, CircularProgress, Box, Button } from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import JobConfigForm, { JobFormData } from '../../components/jobs/JobConfigForm';
import apiClient from '../../services/apiClient';
import { JobConfigDetailDTO } from '../../dto/JobConfigDetailDTO';

const JobEditPage: React.FC = () => {
  const { jobName } = useParams<{ jobName: string }>();
  const navigate = useNavigate();
  const [initialValues, setInitialValues] = useState<JobFormData | undefined>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  useEffect(() => {
    if (!jobName) {
      setError('Job name not found in URL.');
      setLoading(false);
      return;
    }
    const fetchJob = async () => {
      setLoading(true);
      try {
        const response = await apiClient.get<JobConfigDetailDTO>(`/jobconfigs/${jobName}`);
        setInitialValues(response.data as JobFormData); // Cast as form data might be partial/different
      } catch (err: any) {
        setError(err.response?.data?.message || err.message || 'Failed to fetch job details for editing.');
      }
      setLoading(false);
    };
    fetchJob();
  }, [jobName]);

  const handleEditSubmit = async (data: JobFormData) => {
    if (!jobName) return;
    setLoading(true);
    setError(null);
    setValidationErrors([]);
    try {
      const payload: any = { ...data }; // Create a mutable copy
      // jobName is not in the DTO for update usually, it's in path
      // delete payload.jobName; // Or ensure JobConfigUpdateDTO on backend doesn't expect it

      await apiClient.put<JobConfigDetailDTO>(`/jobconfigs/${jobName}`, payload);
      navigate(`/jobs/${jobName}?jobUpdated=true`); // Redirect to detail page
    } catch (err: any) {
      console.error("Update job error:", err);
      if (err.response && err.response.data && err.response.data.message) {
        setError(err.response.data.message);
      } else {
        setError(err.message || 'Failed to update job configuration.');
      }
      setLoading(false);
    }
  };

 const handleValidateWithBackend = async (data: JobFormData): Promise<string[]> => {
    setLoading(true);
    setError(null);
    setValidationErrors([]);
    try {
      // For validation, the DTO might expect jobName, so pass it from path or initialValues
      const dataToValidate = { ...data, jobName: jobName || initialValues?.jobName };
      const response = await apiClient.post<string[]>('/jobconfigs/validate', dataToValidate);
      setLoading(false);
      if (response.data.length === 1 && response.data[0] === "Configuration payload is valid.") {
        setValidationErrors([]);
        return [];
      }
      setValidationErrors(response.data);
      return response.data;
    } catch (err: any) {
      const errMsg = err.response?.data?.message || err.message || 'Failed to validate configuration.';
      setError(errMsg);
      setValidationErrors([errMsg]);
      setLoading(false);
      return [errMsg];
    }
  };

  if (loading && !initialValues) {
    return <Box sx={{ display: 'flex', justifyContent: 'center', my: 3 }}><CircularProgress /></Box>;
  }

  if (error && !initialValues) {
    return <Alert severity="error">{error}</Alert>;
  }

  if (!initialValues && !loading) { // Added !loading to prevent brief "not found" flash
    return <Alert severity="info">Job details not found for '{jobName}'. It might have been deleted or the name is incorrect.</Alert>;
  }

  // Render form only when initialValues are available
  return (
    <Container maxWidth="md">
      <Button component={RouterLink} to={jobName ? `/jobs/${jobName}` : "/"} startIcon={<ArrowBackIcon />} sx={{ mb: 2 }}>
        {jobName ? "Back to Job Details" : "Back to Job List"}
      </Button>
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {validationErrors.length > 0 && validationErrors[0] !== "Configuration payload is valid." && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          <Typography variant="h6">Validation Issues from Backend:</Typography>
          <ul>
            {validationErrors.map((msg, index) => <li key={index}>{msg}</li>)}
          </ul>
        </Alert>
      )}
      {loading && <Box sx={{ position: 'absolute', top: '50%', left: '50%'}}><CircularProgress /></Box>}
      {initialValues && ( // Ensure initialValues are loaded before rendering form
        <JobConfigForm
          onSubmit={handleEditSubmit}
          initialValues={initialValues}
          isEditMode={true}
          onValidate={handleValidateWithBackend}
          jobNameFromPath={jobName} // Pass jobName for display if needed
        />
      )}
    </Container>
  );
};

export default JobEditPage;
