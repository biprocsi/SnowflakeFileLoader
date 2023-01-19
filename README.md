#README

This tool loads file-based datasets into Snowflake instance. The user has control over the data and connections (to the raw files and to the Snowflake instance) via config files.



##Deployment:
  1. create a folder somewhere in your machine and place the both the "SnowflakeFileLoader.py" and "filehelper.py" there
  2. inside the folder above, create a subfolder called "Config"
  3. inside the Config folder place the two config files - "DataConfig.csv" and "ConnectionConfig.csv"
  4. fill out the two config forms (see CONFIGURATION for further details)
  5. deploy the master database, FILE_LOADER_MASTER_DB, by running the script "DeployMasterDatabase.sql" in Snowflake
  
 The tools should now be ready to run.
 
 

##Requirements:
  - the Snowflake instance must be established
  - the files must exist in an accessible location in one of Azure (blob), GCS, S3 or in a folder on your local machine
  - if the files are stored at one of the above three cloud vendors, a Snowflake integration must already be setup
  - Python 3.6+ must be installed on your machine
  - the following Python external libaries must be installed:
    - boto3 ()
    - google.cloud.storage ()
    - azure.storage.blob ()
    - smart_open ()
    - snowflake.connector ()
