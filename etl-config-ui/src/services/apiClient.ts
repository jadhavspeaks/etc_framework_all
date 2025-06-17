import axios from 'axios';

const apiClient = axios.create({
  baseURL: process.env.REACT_APP_API_BASE_URL || 'http://localhost:8080/api/v1',
  // timeout: 1000,
  // headers: {'X-Custom-Header': 'foobar'}
});

// Later, add interceptors to inject Authorization header for Basic Auth or JWT
// apiClient.interceptors.request.use(config => {
//   const token = localStorage.getItem('authToken'); // Example
//   if (token) {
//     config.headers.Authorization = `Bearer ${token}`;
//   }
//   return config;
// });

export default apiClient;
