import React from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import { AppBar, Toolbar, Typography, Container, Button, Box } from '@mui/material';
import JobListPage from './pages/JobListPage';
import JobDetailPage from './pages/JobDetailPage';
import CreateJobPage from './pages/CreateJobPage';
import EditJobPage from './pages/EditJobPage'; // Import EditJobPage

const HomePage = () => <Box sx={{p:2}}><h2>Home</h2><p>Welcome to the ETL Job Configuration UI.</p></Box>;
const NotFoundPage = () => <Box sx={{p:2}}><h2>404 Not Found</h2><p>The page you are looking for does not exist.</p></Box>;

function App() {
  return (
    <div className="App">
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            ETL Job Configurator
          </Typography>
          <Button color="inherit" component={Link} to="/">Home</Button>
          <Button color="inherit" component={Link} to="/jobs">View Jobs</Button>
          <Button color="inherit" component={Link} to="/jobs/create">Create Job</Button>
        </Toolbar>
      </AppBar>
      <Container sx={{ marginTop: '20px', paddingBottom: '20px' }}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/jobs" element={<JobListPage />} />
          <Route path="/jobs/create" element={<CreateJobPage />} />
          <Route path="/jobs/:jobName" element={<JobDetailPage />} />
          <Route path="/jobs/:jobName/edit" element={<EditJobPage />} /> {/* Added route for edit */}
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </Container>
    </div>
  );
}

export default App;
