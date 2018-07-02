BULK INSERT dbo.timesheet
FROM '$(data_file_path)'
WITH (
  FORMATFILE = '$(format_file_path)'
);
