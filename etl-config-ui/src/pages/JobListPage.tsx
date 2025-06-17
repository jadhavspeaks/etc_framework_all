import React, { useEffect, useState, useCallback } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { DataGrid, GridColDef, GridRenderCellParams, GridRowId } from '@mui/x-data-grid';
import { Box, Typography, Button, CircularProgress, Alert, Chip, IconButton, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle, Snackbar, Container } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit'; // For consistency if Edit button is also an icon
import DeleteIcon from '@mui/icons-material/Delete';
import apiClient from '../services/apiClient';
import { useAuth } from '../context/AuthContext'; // Import useAuth

interface JobSummary { id: string; jobName: string; jobDescription?: string; sourceType: string; targetType: string; transformationMode: string; isActive: string; updatedTs?: string; }
interface PaginatedJobResponse { content: JobSummary[]; totalPages: number; totalElements: number; number: number; size: number; }

const JobListPage: React.FC = () => {
  const auth = useAuth(); // Get auth context
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [jobNameToDelete, setJobNameToDelete] = useState<string | null>(null);
  const [snackbar, setSnackbar] = useState<{ open: boolean, message: string, severity: 'success' | 'error' } | null>(null);

  const fetchJobs = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const response = await apiClient.get<PaginatedJobResponse>('/jobconfigs?sort=jobName,asc');
      const mappedData = response.data.content.map(job => ({ ...job, id: job.jobName, isActive: String(job.isActive)}));
      setJobs(mappedData);
    } catch (err: any) { setError(err.message || 'Failed to fetch jobs.'); console.error("Fetch jobs error:", err); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchJobs(); }, [fetchJobs]);

  const handleOpenDeleteDialog = (jobName: string) => { setJobNameToDelete(jobName); setDeleteDialogOpen(true); };
  const handleCloseDeleteDialog = () => { setJobNameToDelete(null); setDeleteDialogOpen(false); };
  const handleDeleteJob = async () => {
    if (!jobNameToDelete) return;
    setLoading(true);
    try {
      await apiClient.delete(`/jobconfigs/${jobNameToDelete}`);
      setSnackbar({ open: true, message: `Job '${jobNameToDelete}' deleted.`, severity: 'success' });
      fetchJobs();
    } catch (err: any) {
      const errMsg = err.response?.data?.message || err.message || 'Delete failed.';
      setSnackbar({ open: true, message: errMsg, severity: 'error' });
    } finally {
      handleCloseDeleteDialog();
      setLoading(false);
    }
  };

  const userRoles = auth.user?.roles || [];
  const canCreateEdit = userRoles.includes('ROLE_EDITOR') || userRoles.includes('ROLE_ADMIN');
  const canDelete = userRoles.includes('ROLE_ADMIN');

  const columns: GridColDef[] = [
    { field: 'jobName', headerName: 'Job Name', width: 250, renderCell: (params: GridRenderCellParams) => (<Button component={RouterLink} to={`/jobs/${params.value}`}>{params.value}</Button>)},
    { field: 'jobDescription', headerName: 'Description', width: 300, flex: 1 },
    { field: 'sourceType', headerName: 'Source Type', width: 130 },
    { field: 'targetType', headerName: 'Target Type', width: 130 },
    { field: 'transformationMode', headerName: 'Mode', width: 130 },
    { field: 'isActive', headerName: 'Active', width: 90, renderCell: (params: GridRenderCellParams) => (String(params.value).toUpperCase() === 'Y' ? <Chip label="Yes" color="success" size="small" /> : <Chip label="No" color="error" size="small" /> )},
    { field: 'updatedTs', headerName: 'Last Updated', width: 180, type: 'dateTime', valueFormatter: (params) => params.value ? new Date(params.value as string).toLocaleString() : '' },
    {
      field: 'actions',
      headerName: 'Actions',
      sortable: false,
      filterable: false,
      width: 150,
      renderCell: (params: GridRenderCellParams) => (
        <Box>
          {canCreateEdit && (
            <IconButton component={RouterLink} to={`/jobs/${params.row.jobName}/edit`} size="small" color="primary" aria-label="edit" title="Edit">
              <EditIcon />
            </IconButton>
          )}
          {canDelete && (
            <IconButton onClick={() => handleOpenDeleteDialog(params.row.jobName as string)} size="small" color="error" aria-label="delete" title="Delete">
              <DeleteIcon />
            </IconButton>
          )}
        </Box>
      ),
    },
  ];

  if (loading && jobs.length === 0) return <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '80vh' }}><CircularProgress /></Box>;
  if (error && jobs.length === 0) return <Alert severity="error">{error}</Alert>; // Show error prominently if no data at all

  return (
    <Container maxWidth="xl">
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h4" component="h1">Job Configurations</Typography>
        {canCreateEdit && (
          <Button variant="contained" color="primary" startIcon={<AddIcon />} component={RouterLink} to="/jobs/new">
            Create New Job
          </Button>
        )}
      </Box>
      {error && jobs.length > 0 && <Alert severity="warning" sx={{mb:2}}>{error} (displaying cached/previous data)</Alert>} {/* Show error as warning if data is stale */}
      <Box sx={{ height: 650, width: '100%' }}><DataGrid rows={jobs} columns={columns} pageSizeOptions={[10,25,50]} initialState={{pagination:{paginationModel:{pageSize:10,page:0}}}} loading={loading} /></Box>
      <Dialog open={deleteDialogOpen} onClose={handleCloseDeleteDialog}><DialogTitle>Confirm Deletion</DialogTitle><DialogContent><DialogContentText>Are you sure you want to delete job "{jobNameToDelete}"?</DialogContentText></DialogContent><DialogActions><Button onClick={handleCloseDeleteDialog}>Cancel</Button><Button onClick={handleDeleteJob} color="error">Delete</Button></DialogActions></Dialog>
      {snackbar && <Snackbar open={snackbar.open} autoHideDuration={6000} onClose={()=>setSnackbar(null)} anchorOrigin={{vertical:'bottom',horizontal:'center'}}><Alert onClose={()=>setSnackbar(null)} severity={snackbar.severity} sx={{width:'100%'}}>{snackbar.message}</Alert></Snackbar>}
    </Container>
  );
};

export default JobListPage;
