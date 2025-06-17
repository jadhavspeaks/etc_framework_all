import React, { useEffect, useState } from 'react';
import { Box, Typography, CircularProgress, Alert, Paper, TableContainer, Table, TableHead, TableRow, TableCell, TableBody, TablePagination } from '@mui/material';
import apiClient from '../../services/apiClient';

interface AuditLogEntry {
  logId: number;
  jobName: string;
  changeType: string;
  changedByUser: string;
  changeTimestamp: string; // Assuming string timestamp from API
  changedFieldsDetails?: string; // JSON string
}

interface PaginatedAuditLogResponse {
  content: AuditLogEntry[];
  totalElements: number;
  totalPages: number;
  number: number; // current page (0-indexed)
  size: number;
}

interface AuditLogTabContentProps {
  jobName: string;
}

const AuditLogTabContent: React.FC<AuditLogTabContentProps> = ({ jobName }) => {
  const [auditLogs, setAuditLogs] = useState<AuditLogEntry[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [totalRows, setTotalRows] = useState(0);

  useEffect(() => {
    const fetchAuditLogs = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await apiClient.get<PaginatedAuditLogResponse>(
          `/jobconfigs/${jobName}/auditlogs?page=${page}&size=${rowsPerPage}&sort=changeTimestamp,desc`
        );
        setAuditLogs(response.data.content);
        setTotalRows(response.data.totalElements);
      } catch (err: any) {
        setError(err.response?.data?.message || err.message || 'Failed to fetch audit logs.');
        console.error("Error fetching audit logs:", err);
      }
      setLoading(false);
    };

    if (jobName) {
      fetchAuditLogs();
    }
  }, [jobName, page, rowsPerPage]);

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  if (loading) {
    return <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}><CircularProgress /></Box>;
  }

  if (error) {
    return <Alert severity="error" sx={{ mt: 2 }}>{error}</Alert>;
  }

  if (auditLogs.length === 0) {
    return <Typography sx={{ mt: 2 }}>No audit log entries found for this job configuration.</Typography>;
  }

  return (
    <Paper sx={{ mt: 2 }}>
      <TableContainer>
        <Table stickyHeader aria-label="audit log table">
          <TableHead>
            <TableRow>
              <TableCell>Timestamp</TableCell>
              <TableCell>User</TableCell>
              <TableCell>Change Type</TableCell>
              <TableCell>Details</TableCell> {/* Expandable or modal for JSON details */}
            </TableRow>
          </TableHead>
          <TableBody>
            {auditLogs.map((log) => (
              <TableRow hover key={log.logId}>
                <TableCell>{new Date(log.changeTimestamp).toLocaleString()}</TableCell>
                <TableCell>{log.changedByUser}</TableCell>
                <TableCell>{log.changeType}</TableCell>
                <TableCell>
                  {/* Basic display, could be a button to show full JSON in a modal */}
                  <pre style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: '100px', overflow: 'auto' }}>
                    {log.changedFieldsDetails ? JSON.stringify(JSON.parse(log.changedFieldsDetails), null, 2) : 'N/A'}
                  </pre>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TablePagination
        rowsPerPageOptions={[5, 10, 25]}
        component="div"
        count={totalRows}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </Paper>
  );
};

export default AuditLogTabContent;
