import axios from 'axios';

const API_BASE_URL = '/api/v1/jobconfigs'; // Uses proxy in development

let basicAuthHeader = null;

export const setAuthCredentials = (username, password) => {
    if (username && password) {
        basicAuthHeader = 'Basic ' + window.btoa(username + ":" + password);
    } else {
        basicAuthHeader = null; // Clear if no credentials
    }
};

export const clearAuthCredentials = () => {
    basicAuthHeader = null;
};

const getRequestConfig = () => {
    const config = {};
    if (basicAuthHeader) {
        config.headers = { 'Authorization': basicAuthHeader };
    }
    return config;
};

export const fetchJobConfigs = async (page = 0, size = 10, sort = 'jobName,asc', jobNameFilter = '', sourceTypeFilter = '') => {
    try {
        const params = { page, size, sort };
        if (jobNameFilter) params.jobNameFilter = jobNameFilter;
        if (sourceTypeFilter) params.sourceTypeFilter = sourceTypeFilter;

        const response = await axios.get(API_BASE_URL, {
            ...getRequestConfig(),
            params: params
        });
        return response.data; // Spring Boot Page object
    } catch (error) {
        console.error('Error fetching job configs:', error.response || error.message);
        throw error;
    }
};

export const fetchJobConfigByName = async (jobName) => {
    try {
        const response = await axios.get(`${API_BASE_URL}/${jobName}`, getRequestConfig());
        return response.data;
    } catch (error) {
        console.error(`Error fetching job config ${jobName}:`, error.response || error.message);
        throw error;
    }
};

export const createJobConfig = async (jobData) => {
    try {
        const response = await axios.post(API_BASE_URL, jobData, getRequestConfig());
        return response.data;
    } catch (error) {
        console.error('Error creating job config:', error.response || error.message);
        throw error;
    }
};

export const updateJobConfig = async (jobName, jobData) => {
    try {
        const response = await axios.put(`${API_BASE_URL}/${jobName}`, jobData, getRequestConfig());
        return response.data;
    } catch (error) {
        console.error(`Error updating job config ${jobName}:`, error.response || error.message);
        throw error;
    }
};

// Placeholder for deleteJobConfig, validateJobConfig
export const deleteJobConfig = async (jobName) => {
    try {
        await axios.delete(`${API_BASE_URL}/${jobName}`, getRequestConfig());
        // Delete usually returns 204 No Content, so no response.data to return
    } catch (error) {
        console.error(`Error deleting job config ${jobName}:`, error.response || error.message);
        throw error;
    }
};

export const validateJobConfigPayload = async (jobData) => {
    try {
        const response = await axios.post(`${API_BASE_URL}/validate`, jobData, getRequestConfig());
        return response.data; // Expects List<String> or similar validation messages
    } catch (error) {
        console.error('Error validating job config payload:', error.response || error.message);
        // If backend returns 400 with error details in response.data, that might be useful here
        // For now, just rethrow. Error handling in component should be more specific.
        throw error;
    }
};
