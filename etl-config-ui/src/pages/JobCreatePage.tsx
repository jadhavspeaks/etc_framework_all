import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Container, Typography, Alert, Box, CircularProgress } from '@mui/material';
import JobConfigForm, { JobFormData } from '../../components/jobs/JobConfigForm';
import apiClient from '../../services/apiClient';
import { JobConfigDetailDTO } from '../../dto/JobConfigDetailDTO'; // Create this DTO file

const JobCreatePage: React.FC = () => {
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  const handleCreateSubmit = async (data: JobFormData) => {
    setLoading(true);
    setError(null);
    setValidationErrors([]);
    try {
      // Remove null/undefined fields before sending, or ensure DTO handles them
      const payload: any = {};
      for (const key in data) {
        if ((data as any)[key] !== null && (data as any)[key] !== undefined) {
          payload[key] = (data as any)[key];
        }
      }
      await apiClient.post<JobConfigDetailDTO>('/jobconfigs', payload);
      navigate('/?jobCreated=' + payload.jobName); // Redirect to list page with a success query param
    } catch (err: any) {
      console.error("Create job error:", err);
      if (err.response && err.response.data && err.response.data.errors) {
        // Handle Spring Boot validation errors if customized
        setError('Validation errors occurred.');
        // setValidationErrors(err.response.data.errors.map((e:any) => e.defaultMessage || e.message));
      } else if (err.response && err.response.data && err.response.data.message) {
        setError(err.response.data.message);
      } else {
        setError(err.message || 'Failed to create job configuration.');
      }
      setLoading(false);
    }
  };

  const handleValidateWithBackend = async (data: JobFormData): Promise<string[]> => {
    setLoading(true);
    setError(null);
    setValidationErrors([]);
    try {
      const response = await apiClient.post<string[]>('/jobconfigs/validate', data);
      setLoading(false);
      if (response.data.length === 1 && response.data[0] === "Configuration payload is valid.") {
        setValidationErrors([]);
        return [];
      }
      setValidationErrors(response.data);
      return response.data;
    } catch (err: any) {
      console.error("Validate job error:", err);
      const errMsg = err.response?.data?.message || err.message || 'Failed to validate configuration.';
      setError(errMsg);
      setValidationErrors([errMsg]);
      setLoading(false);
      return [errMsg];
    }
  };

  return (
    <Container maxWidth="md">
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {validationErrors.length > 0 && validationErrors[0] !== "Configuration payload is valid." && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          <Typography variant="h6">Validation Issues from Backend:</Typography>
          <ul>
            {validationErrors.map((msg, index) => <li key={index}>{msg}</li>)}
          </ul>
        </Alert>
      )}
      {loading && <Box sx={{ display: 'flex', justifyContent: 'center', my:2 }}><CircularProgress /></Box>}
      <JobConfigForm
        onSubmit={handleCreateSubmit}
        onValidate={handleValidateWithBackend}
      />
    </Container>
  );
};

export default JobCreatePage;
