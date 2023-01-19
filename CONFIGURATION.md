# CONFIGURATION

The user controls the tool via the two config files:
  - ConnectionConfig lists the connection credentials for both the source (raw data files) and destination (your Snowflake instance)
  - DataConfig drives the logical mapping between your files and tables

## ConnectionConfig
You must populate the following fields:
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
