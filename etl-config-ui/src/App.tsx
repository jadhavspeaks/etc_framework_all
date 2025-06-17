import React from 'react';
import { Routes, Route, Link as RouterLink, useNavigate, useLocation, Navigate } from 'react-router-dom'; // Added Navigate
import { AppBar, Toolbar, Typography, Container, Button, Box, IconButton } from '@mui/material';
import LogoutIcon from '@mui/icons-material/Logout';
import JobListPage from './pages/JobListPage';
import JobDetailPage from './pages/JobDetailPage';
import JobCreatePage from './pages/JobCreatePage';
import JobEditPage from './pages/JobEditPage';
import LoginPage from './pages/LoginPage'; // Import LoginPage
import { useAuth } from './context/AuthContext'; // Import useAuth

const NotFoundPage = () => <Box sx={{p:2}}><Typography variant="h5">404 - Page Not Found</Typography></Box>;

// ProtectedRoute component (can be in a separate file e.g., components/ProtectedRoute.tsx)
const ProtectedRoute: React.FC<{ children: JSX.Element }> = ({ children }) => {
  const auth = useAuth();
  const location = useLocation();

  if (!auth.isAuthenticated) {
    // Redirect them to the /login page, but save the current location they were
    // trying to go to when they were redirected. This allows us to send them
    // along to that page after they login, which is a nicer user experience
    // than dropping them off on the home page.
    return <Navigate to="/login" state={{ from: location }} replace />;
  }
  return children;
};

function App() {
  const auth = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    auth.logout();
    navigate('/login'); // Redirect to login after logout
  };

  return (
    <>
      {auth.isAuthenticated && (
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
              <Button component={RouterLink} to="/" sx={{ color: 'inherit' }}>
                ETL Config UI
              </Button>
            </Typography>
            <Typography sx={{ mr: 2 }}>User: {auth.user?.username}</Typography>
            <IconButton color="inherit" onClick={handleLogout} aria-label="logout">
                <LogoutIcon />
            </IconButton>
          </Toolbar>
        </AppBar>
      )}
      <Container maxWidth={false} sx={{ marginTop: '20px', paddingX: '20px' }}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />

          {/* Protected Routes */}
          <Route path="/" element={<ProtectedRoute><JobListPage /></ProtectedRoute>} />
          <Route path="/jobs/new" element={<ProtectedRoute><JobCreatePage /></ProtectedRoute>} />
          <Route path="/jobs/:jobName" element={<ProtectedRoute><JobDetailPage /></ProtectedRoute>} />
          <Route path="/jobs/:jobName/edit" element={<ProtectedRoute><JobEditPage /></ProtectedRoute>} />

          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </Container>
    </>
  );
}

export default App;
