import React from 'react';
import { useForm, Controller, SubmitHandler, UseFormReturn } from 'react-hook-form';
import { TextField, Button, Grid, Paper, Typography, Select, MenuItem, FormControl, InputLabel, FormHelperText, Box, Divider, Chip } from '@mui/material'; // Removed Autocomplete as it's not used in the provided code
import { JobConfigDetailDTO } from '../../dto/JobConfigDetailDTO'; // Assuming DTO definition

export type JobFormData = Partial<JobConfigDetailDTO>;

interface JobConfigFormProps {
  onSubmit: SubmitHandler<JobFormData>;
  initialValues?: JobFormData;
  isEditMode?: boolean;
  onValidate?: (data: JobFormData) => Promise<string[]>;
  jobNameFromPath?: string; // For edit mode to confirm jobName if it's in form data
}

// Example enum-like values (could be imported from a constants file)
const transformationModes = ["AS_IS", "WITH_LOGIC", "SCD2"];
const sourceTypes = ["FILE", "HIVE_TABLE"]; // Add KAFKA, JDBC later
const targetTypes = ["FILE", "HDFS", "HIVE_TABLE"]; // Add KAFKA, ORACLE_TABLE later
const fileFormats = ["CSV", "PARQUET", "JSON", "ORC", "TEXT", "AVRO"];
const loadTypes = ["INCREMENTAL", "FULL_RELOAD"];
const writeModes = ["overwrite", "append", "ignore", "errorifexists"];
const yesNo = ["Y", "N"];
const auditLevels = ["NONE", "JOB", "RECORD"];
const notificationVerbosityOpts = ["BRIEF", "DETAILED"];

const JobConfigForm: React.FC<JobConfigFormProps> = ({
    onSubmit,
    initialValues = {},
    isEditMode = false,
    onValidate,
    jobNameFromPath
  }) => {
  const formMethods: UseFormReturn<JobFormData> = useForm<JobFormData>({
    defaultValues: initialValues,
    mode: 'onChange', // Validate on change for better UX
  });
  const { control, handleSubmit, formState: { errors }, watch, reset, getValues } = formMethods;

  const transformationMode = watch('transformationMode');
  const sourceType = watch('sourceType');
  const targetType = watch('targetType');
  const dqChecksEnabled = watch('dqChecksEnabled');
  const reconciliationEnabled = watch('reconciliationEnabled');

  React.useEffect(() => {
    // If initialValues are loaded (e.g., for edit), reset the form
    // Ensure all fields in initialValues are correctly mapped
    const transformedInitialValues = {
        ...initialValues,
        jobPriority: initialValues.jobPriority ?? 0,
        maxRetries: initialValues.maxRetries ?? 3,
        retryBackoffMs: initialValues.retryBackoffMs ?? 60000,
        // Ensure boolean-like Y/N fields are strings if Select expects strings
        isActive: initialValues.isActive || 'Y',
        reconciliationEnabled: initialValues.reconciliationEnabled || 'N',
        dqChecksEnabled: initialValues.dqChecksEnabled || 'N',
        executionAuthorizationFlag: initialValues.executionAuthorizationFlag || 'Y',
        manualTriggerOnly: initialValues.manualTriggerOnly || 'N',
    };
    reset(transformedInitialValues);
  }, [initialValues, reset]);

  const handleValidate = async () => {
    if (onValidate) {
      const currentData = getValues();
      const validationErrors = await onValidate(currentData);
      if (validationErrors.length === 0) {
        alert('Backend validation successful!');
      } else {
        alert('Backend validation errors:\n' + validationErrors.join('\n'));
      }
    }
  };

  const renderTextField = (name: keyof JobFormData, label: string, required: boolean = false, rules: any = {}, otherProps: any = {}) => (
    <Grid item xs={12} sm={otherProps.sm || 6} md={otherProps.md || 4}>
      <Controller
        name={name}
        control={control}
        rules={rules}
        defaultValue={initialValues?.[name] || ''} // Ensure default value is set
        render={({ field }) => (
          <TextField {...field} label={label} variant="outlined" fullWidth required={required}
                     error={!!errors[name]} helperText={(errors[name] as any)?.message} {...otherProps} />
        )}
      />
    </Grid>
  );

  const renderSelectField = (name: keyof JobFormData, label: string, options: string[], required: boolean = false, rules: any = {}) => (
    <Grid item xs={12} sm={6} md={4}>
      <Controller
        name={name}
        control={control}
        rules={rules}
        defaultValue={initialValues?.[name] || (options.includes('N') ? 'N' : options.includes('AS_IS') ? 'AS_IS': options.length > 0 ? options[0] : '')} // Sensible default
        render={({ field }) => (
          <FormControl fullWidth error={!!errors[name]} required={required}>
            <InputLabel>{label}</InputLabel>
            <Select {...field} label={label}>
              {options.map(opt => <MenuItem key={opt} value={opt}>{opt}</MenuItem>)}
            </Select>
            {errors[name] && <FormHelperText>{(errors[name] as any)?.message}</FormHelperText>}
          </FormControl>
        )}
      />
    </Grid>
  );

 const renderYesNoSelect = (name: keyof JobFormData, label: string, required: boolean = false, rules: any = {}) => renderSelectField(name, label, yesNo, required, rules);

  return (
    <Paper elevation={3} sx={{ p: 3, mt:2 }}>
      <Typography variant="h5" gutterBottom>
        {isEditMode ? `Edit Job: ${initialValues?.jobName || jobNameFromPath}` : 'Create New Job Configuration'}
      </Typography>
      <form onSubmit={handleSubmit(onSubmit)} noValidate>
        <Grid container spacing={3}>
          {/* Section 1: Core Identification */}
          <Grid item xs={12}><Divider><Chip label="Core Identification" /></Divider></Grid>
          {renderTextField('jobName', 'Job Name', true, { required: 'Job Name is required', pattern: { value: /^[a-zA-Z0-9_.-]+$/, message: 'Alphanumeric, _, ., - only' }}, { sm:12, md:12, disabled: isEditMode })}
          {renderTextField('jobDescription', 'Job Description', false, {}, { sm:12, md:12, multiline: true, rows: 2 })}
          {renderYesNoSelect('isActive', 'Is Active?', true, { required: 'isActive is required.'})}

          {/* Section 2: Source Configuration */}
          <Grid item xs={12}><Divider sx={{mt:2}}><Chip label="Source Configuration" /></Divider></Grid>
          {renderSelectField('sourceType', 'Source Type', sourceTypes, true, { required: 'Source Type is required.' })}
          {renderTextField('sourceConnectionDetails', 'Source Connection Details', true, { required: 'Source Connection is required.' })}
          {(sourceType === 'FILE') && (
            <>
              {renderSelectField('sourceFormat', 'Source Format', fileFormats, true, { required: 'Source Format is required for FILE type.' })}
              {renderTextField('sourceFormatOptions', 'Source Format Options (JSON)', false, {}, { multiline: true, rows: 2, sm:12, md:12 })}
            </>
          )}
          {renderTextField('sourceSchema', 'Source Schema (CLOB/DDL)', false, {}, { multiline: true, rows: 4, sm:12, md:12 })}

          {/* Section 3: Target Configuration */}
          <Grid item xs={12}><Divider sx={{mt:2}}><Chip label="Target Configuration" /></Divider></Grid>
          {renderSelectField('targetType', 'Target Type', targetTypes, true, { required: 'Target Type is required.' })}
          {renderTextField('targetConnectionDetails', 'Target Connection Details')}
          {renderTextField('targetTableOrPath', 'Target Table or Path', true, { required: 'Target Table/Path is required.' })}
          {((targetType === 'FILE' || targetType === 'HDFS') || (targetType === 'HIVE_TABLE' && initialValues?.targetFormatOptions?.trim())) && ( // Corrected logic for initialValues
             // Hive target usually doesn't need format, but allow if specified for some reason
            <>
            {renderSelectField('targetFormat', 'Target Format', fileFormats, targetType !== 'HIVE_TABLE', {})}
            {renderTextField('targetFormatOptions', 'Target Format Options (JSON)', false, {}, { multiline: true, rows: 2, sm:12, md:12 })}
            </>
          )}
          {renderSelectField('loadType', 'Load Type', loadTypes, true, { required: 'Load Type is required.' })}
          {renderSelectField('targetWriteMode', 'Target Write Mode', writeModes, true, { required: 'Write Mode is required.' })}
          {renderTextField('partitioningColumns', 'Partitioning Columns (comma-sep)')}

          {/* Section 4: Transformation */}
          <Grid item xs={12}><Divider sx={{mt:2}}><Chip label="Transformation Logic" /></Divider></Grid>
          {renderSelectField('transformationMode', 'Transformation Mode', transformationModes, true, { required: 'Mode is required.' })}
          {(transformationMode === 'WITH_LOGIC' || transformationMode === 'SCD2') && (
            renderTextField('sqlLogic', 'SQL Logic (CLOB)', true, { required: 'SQL Logic is required for this mode.' }, { multiline: true, rows: 6, sm:12, md:12 })
          )}
          {renderTextField('schemaMappingLogic', 'Schema Mapping Logic (JSON CLOB)', false, {}, { multiline: true, rows: 4, sm:12, md:12 })}

          {/* Section 5: SCD Type 2 (Conditional) */}
          {transformationMode === 'SCD2' && (
            <>
              <Grid item xs={12}><Divider sx={{mt:2}}><Chip label="SCD Type 2 Configuration" /></Divider></Grid>
              {renderTextField('scd2NaturalKeys', 'SCD2 Natural Keys (comma-sep)', true, { required: 'SCD2 Natural Keys are required.' })}
              {renderTextField('scd2ChangeTrackingColumn', 'SCD2 Change Tracking Column')}
              {renderTextField('scd2SurrogateKeyColumn', 'SCD2 Surrogate Key Column Name')}
              {renderTextField('scd2ValidFromColumn', 'SCD2 Valid From Column Name')}
              {renderTextField('scd2ValidToColumn', 'SCD2 Valid To Column Name')}
              {renderTextField('scd2VersionColumn', 'SCD2 Version Column Name')}
              {renderTextField('scd2CurrentFlagColumn', 'SCD2 Current Flag Column Name')}
            </>
          )}

          <Grid item xs={12}><Divider sx={{mt:2}}><Chip label="Execution & Operational" /></Divider></Grid>
          {renderTextField('jobPriority', 'Job Priority', false, {}, {type: 'number'})}
          {renderTextField('maxRetries', 'Max Retries', false, {}, {type: 'number'})}
          {renderTextField('retryBackoffMs', 'Retry Backoff (ms)', false, {}, {type: 'number'})}
          {renderTextField('slaThresholdMinutes', 'SLA Threshold (mins)', false, {}, {type: 'number'})}
          {renderTextField('dependencyJobIds', 'Dependency Job Names (comma-sep)')}
          {renderSelectField('auditLevel', 'Audit Level', auditLevels, true, {})}
          {renderYesNoSelect('reconciliationEnabled', 'Reconciliation Enabled?')}
          {reconciliationEnabled === 'Y' && renderTextField('reconciliationConfig', 'Recon Config (JSON)', true, {required: 'Recon Config is required if enabled.'}, {multiline:true, rows:3, sm:12, md:12})}
          {renderYesNoSelect('dqChecksEnabled', 'DQ Checks Enabled?')}
          {dqChecksEnabled === 'Y' && renderTextField('dqRulesConfig', 'DQ Rules (JSON)', true, {required: 'DQ Rules are required if enabled.'}, {multiline:true, rows:4, sm:12, md:12})}
          {renderTextField('notificationEmailsSuccess', 'Notification Emails (Success)')}
          {renderTextField('notificationEmailsFailure', 'Notification Emails (Failure)')}
          {renderSelectField('notificationVerbosity', 'Notification Verbosity', notificationVerbosityOpts, false, {})}
          {renderTextField('jobProperties', 'Job Properties (JSON)', false, {}, {multiline:true, rows:3, sm:12, md:12})}
          {renderTextField('logMaskingColumns', 'Log Masking Columns (comma-sep)')}
          {renderYesNoSelect('executionAuthorizationFlag', 'Execution Auth Flag', true, {})}
          {renderYesNoSelect('manualTriggerOnly', 'Manual Trigger Only', true, {})}

          <Grid item xs={12} display="flex" justifyContent="flex-end" gap={2} sx={{mt:2}}>
            {onValidate && (
              <Button onClick={async () => { if(onValidate) { const valid = await onValidate(getValues()); if(valid.length === 0 || valid[0]==="Configuration payload is valid.") alert("Backend validation passed!"); else alert("Backend validation issues:\n"+valid.join("\n")); } } } variant="outlined" color="info">
                Validate (Backend)
              </Button>
            )}
            <Button type="submit" variant="contained" color="primary">
              {isEditMode ? 'Save Changes' : 'Create Job'}
            </Button>
          </Grid>
        </Grid>
      </form>
    </Paper>
  );
};

export default JobConfigForm;
