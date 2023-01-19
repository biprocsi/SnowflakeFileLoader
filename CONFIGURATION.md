# CONFIGURATION

The user controls the tool via the two config files:
  - ConnectionConfig lists the connection credentials for both the source (raw data files) and target (your Snowflake instance)
  - DataConfig drives the logical mapping between your files and tables

## ConnectionConfig
You must populate the following values:
  - _Source_ (one of 'Azure', 'GCS', 'S3' or'Local')
  - _Bucket_ (your cloud bucket or local parent folder)
  - _Prefix_ (your cloud/local subfolder (if applicable))
  - _SnowflakeIntegration_
  - _SnowflakeAccountId_
  - _SnowflakeUsername_
  - _SnowflakePassword_
  - _SnowflakeWarehouse_

If importing from the cloud you must also provide the relevant credentials below (you can leave the rest blank):
  - _AWSAccessKeyId_
  - _AWSSecretAccessKey_
  - _GCSServiceAccountPath_
  - _AzureConnectionString_
  - _AzureTenantId_


## DataConfig
You must populate the following fields:
  - _FilePattern_ (the common file pattern for this type of file to be loaded)
  - _FileType_ (e.g. 'csv')
  - _Delimiter_ (the column delimiter for this type of file)
    - if the delimiter is ',' then you must use 'comma'
    - if the delimiter is '|' then you must use 'pipe'
  - _HasHeader_ (Y/N)
  - _DatabaseName_ (the target database)
  - _SchemaName_ (the target schema)
  - _TableName_ (the target table)

You can optionally provide a comma-seperated list of the columns by which you would like the table to be clustered
  - _ClusterByCols_ (e.g. 'my_date_col,my_name_col')
