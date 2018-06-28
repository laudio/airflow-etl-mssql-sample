PRINT 'Creating link server with target IP "$(HOST_IP)"';

EXEC sp_addlinkedserver
  @srvproduct= N'',
  @provider = N'SQLNCLI',
  @datasrc = '$(HOST_IP)',
  @server  =  'ETL_SERVER';
