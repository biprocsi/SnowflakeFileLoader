import os
import filehelper
import snowflake.connector
from time import sleep, time

global WORKER_WAREHOUSE
global MASTER_DATABASE
global MASTER_SCHEMA
global MASTER_STAGE
global MASTER_FILE_FORMAT
global MASTER_LOAD_LOG
global MASTER_FAILED_LOAD_LOG
WORKER_WAREHOUSE = 'FILE_LOADER_WH'
MASTER_DATABASE = 'FILE_LOADER_MASTER_DB'
MASTER_SCHEMA = 'ELT'
MASTER_STAGE = 'FL_STG'
MASTER_FILE_FORMAT = 'FL_FF'
MASTER_LOAD_LOG = 'FL_LOG'
MASTER_FAILED_LOAD_LOG = 'FL_FAILED_LOAD'

def readConfig(filename):
    filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Config', filename + '.csv')

    with open(filepath, 'r') as file:
        lines = file.readlines()

    return list(map(lambda line: line.replace('\n', '').split('|'), lines))[1 :]

def delimiterMap(delimiter):
    if delimiter.upper() == 'COMMA':
        return ','
    elif delimiter.upper() == 'PIPE':
        return '|'        
    return delimiter

def standardizedName(objName):
    return objName.replace(' ', '_').replace('-', '_').upper()

class Entity():
    def __init__(self, filePattern, fileType, delimiter, hasHeader, databaseName, schemaName, tableName, clusterByCols):
        self.filePattern = filePattern
        self.fileType = fileType
        self.delimiter = delimiter
        self.hasHeader = hasHeader
        self.databaseName = databaseName
        self.schemaName = schemaName
        self.tableName = tableName
        self.clusterByCols = clusterByCols
        self.fileFormatName = None
        self.hasAtLeastOneNonEmptyFile = False

    def display(self):
        string = ''
        string += 'FilePattern    : {}\n'.format(self.filePattern)
        string += 'FileType       : {}\n'.format(self.fileType)
        string += 'Delimiter      : {}\n'.format(self.delimiter)
        string += 'HasHeader      : {}\n'.format(self.hasHeader)
        string += 'DatabaseName   : {}\n'.format(self.databaseName)
        string += 'SchemaName     : {}\n'.format(self.schemaName)
        string += 'TableName      : {}\n'.format(self.tableName)
        string += 'ClusterByCols  : {}\n'.format(self.clusterByCols)
        string += 'FileFormatName : {}\n'.format(self.fileFormatName)
        print(string)

class Loader():
    def __init__(self):
        self.state = 0
        self.source = None
        self.bucket = None
        self.prefix = None
        self.awsAccessKeyId = None
        self.awsSecretAccessKey = None
        self.gcsServiceAccountPath = None
        self.azureConnectionString = None
        self.azureTenantId = None
        self.snowflakeIntegration = None
        self.snowflakeAccountId = None
        self.snowflakeUsername = None
        self.snowflakePassword = None
        self.snowflakeWarehouse = None
        self.entities = []
        self.databasesToCreate = set()
        self.schemasToCreate = set()
        self.fileFormatsToCreate = dict()

    def display(self):
        print('\n----- Loader -----\n')

        string = ''
        string += 'Source                : {}\n'.format(self.source)
        string += 'Bucket                : {}\n'.format(self.bucket)
        string += 'Prefix                : {}\n'.format(self.prefix)
        string += 'AWSAccessKeyId        : {}\n'.format(self.awsAccessKeyId)
        string += 'AWSSecretAccessKey    : {}\n'.format(self.awsSecretAccessKey)
        string += 'GCSServiceAccountPath : {}\n'.format(self.gcsServiceAccountPath)
        string += 'AzureConnectionString : {}\n'.format(self.azureConnectionString)
        string += 'AzureTenantId         : {}\n'.format(self.azureTenantId)
        string += 'SnowflakeIntegration  : {}\n'.format(self.snowflakeIntegration)
        string += 'SnowflakeAccountId    : {}\n'.format(self.snowflakeAccountId)
        string += 'SnowflakeUsername     : {}\n'.format(self.snowflakeUsername)
        string += 'SnowflakePassword     : {}\n'.format(self.snowflakePassword)
        string += 'SnowflakeWarehouse    : {}\n'.format(self.snowflakeWarehouse)
        print(string)
        print('\n\n---- Entities ----\n')

        for entity in self.entities:
            entity.display()

    def setAttributes(self, configLines):
        pairs = {}

        for pair in configLines:
            pairs[pair[0]] = pair[1]

        self.source = pairs['Source']
        self.bucket = pairs['Bucket']
        self.prefix = pairs['Prefix']
        self.awsAccessKeyId = pairs['AWSAccessKeyId']
        self.awsSecretAccessKey = pairs['AWSSecretAccessKey']
        self.gcsServiceAccountPath = pairs['GCSServiceAccountPath']
        self.azureConnectionString = pairs['AzureConnectionString']
        self.azureTenantId = pairs['AzureTenantId']
        self.snowflakeIntegration = pairs['SnowflakeIntegration']
        self.snowflakeAccountId = pairs['SnowflakeAccountId']
        self.snowflakeUsername = pairs['SnowflakeUsername']
        self.snowflakePassword = pairs['SnowflakePassword']
        self.snowflakeWarehouse = pairs['SnowflakeWarehouse']

        self.source = self.source.upper()

        if self.bucket.endswith('/'):
            self.bucket = self.bucket[: -1]

        if self.prefix.endswith('/'):
            self.prefix = self.prefix[: -1]

    def setEntities(self, configLines):
        for line in configLines:
            cols = list(filter(lambda col: len(col) > 0, line[7].split(',')))
            cols = list(map(lambda col: standardizedName(col), cols))
            self.entities.append(Entity(
                filePattern = line[0],
                fileType = line[1],
                delimiter = delimiterMap(line[2]),
                hasHeader = line[3],
                databaseName = standardizedName(line[4]),
                schemaName = standardizedName(line[5]),
                tableName = standardizedName(line[6]),
                clusterByCols = cols
            ))

    def setObjectsToCreate(self):
        for entity in self.entities:
            key = entity.databaseName

            self.databasesToCreate.add(key)

            key = (
                entity.databaseName,
                entity.schemaName
                )

            self.schemasToCreate.add(key)

            key = (
                entity.databaseName,
                entity.schemaName,
                entity.fileType,
                entity.hasHeader,
                entity.delimiter
                )
            
            if key not in self.fileFormatsToCreate:
                self.fileFormatsToCreate[key] = len(self.fileFormatsToCreate) + 1

    def assignFileFormats(self):
        for entity in self.entities:
            key = (
                entity.databaseName,
                entity.schemaName,
                entity.fileType,
                entity.hasHeader,
                entity.delimiter
                )

            entity.fileFormatName = '{}_{}'.format(MASTER_FILE_FORMAT, self.fileFormatsToCreate[key])

    def drawSqlCreateWarehouse(self, warehouseName):
        string = 'CREATE OR REPLACE WAREHOUSE {}'.format(warehouseName)
        string += '\n    WAREHOUSE_SIZE = \'X-LARGE\''
        string += '\n    INITIALLY_SUSPENDED = true'
        return string

    def drawSqlDropWarehouse(self, warehouseName):
        string = 'DROP WAREHOUSE IF EXISTS {}'.format(warehouseName)
        return string

    def drawSqlUseWarehouse(self, warehouseName):
        return 'USE WAREHOUSE {}'.format(warehouseName)

    def drawSqlCreateDatabase(self, databaseName):
        return 'CREATE OR REPLACE DATABASE {}'.format(databaseName)

    def drawSqlCreateSchema(self, databaseName, schemaName):
        return 'CREATE OR REPLACE SCHEMA {}.{}'.format(databaseName, schemaName)

    def drawSqlCreateTable(self, databaseName, schemaName, tableName, headers):
        headers = list(map(lambda col: standardizedName(col) + ' VARCHAR', headers))
        string = 'CREATE OR REPLACE TABLE {}.{}.{} ('.format(databaseName, schemaName, tableName)
        string += '\n    '
        string += '\n    ,'.join(headers)
        string += '\n)'
        return string

    def drawSqlCreateInternalStage(self):
        return 'CREATE OR REPLACE STAGE {}.{}.{}'.format(MASTER_DATABASE, MASTER_SCHEMA, MASTER_STAGE)

    def drawSqlCreateExternalStage(self, source, integrationName, bucket, prefix):
        string = 'CREATE OR REPLACE STAGE {}.{}.{}'.format(MASTER_DATABASE, MASTER_SCHEMA, MASTER_STAGE)
        string += '\n    STORAGE_INTEGRATION = {}'.format(integrationName)

        if source == 'S3':
            url = 's3://{}/{}/'.format(bucket, prefix)
        elif source == 'GCS':
            url = 'gcs://{}/{}'.format(bucket, prefix)
        else:
            url = 'azure://{}.blob.core.windows.net/{}/'.format(bucket, prefix)

        string += '\n    URL = \'{}\''.format(url)
        return string

    def drawSqlCreateFileFormat(self, databaseName, schemaName, fileType, hasHeader, delimiter, number):
        string = 'CREATE OR REPLACE FILE FORMAT {}.{}.{}_{}'.format(databaseName, schemaName, MASTER_FILE_FORMAT, number)
        string += '\n    TYPE = \'{}\''.format(fileType)
        string += '\n    FIELD_DELIMITER = \'{}\''.format(delimiter)
        string += '\n    FIELD_OPTIONALLY_ENCLOSED_BY = \'"\''
        string += '\n    SKIP_HEADER = {}'.format(str(1 if hasHeader == 'Y' else 0))
        return string

    def drawSqlPutFiles(self, folder, fileType):
        toCompress = ('zip' not in fileType) and ('gz' not in fileType)
        string = 'PUT \'FILE://{}/*.{}\''.format(folder, fileType)
        string += '\n    @{}.{}.{}'.format(MASTER_DATABASE, MASTER_SCHEMA, MASTER_STAGE)
        string += '\n    AUTO_COMPRESS = {}'.format('TRUE' if toCompress else 'FALSE')
        return string

    def drawSqlCopyInto(self, databaseName, schemaName, tableName, filePattern, fileFormatName):
        string = 'COPY INTO {}.{}.{}'.format(databaseName, schemaName, tableName)
        string += '\nFROM @{}.{}.{}'.format(MASTER_DATABASE, MASTER_SCHEMA, MASTER_STAGE)
        string += '\n    PATTERN = \'{}\''.format(filePattern)
        string += '\n    FILE_FORMAT = {}.{}.{}'.format(databaseName, schemaName, fileFormatName)
        string += '\n    ON_ERROR = SKIP_FILE'
        return string

    def drawSqlReorderTable(self, databaseName, schemaName, tableName, clusterByCols):
        string = 'CREATE OR REPLACE TABLE {}.{}.{} AS ('.format(databaseName, schemaName, tableName)
        string += '\n    SELECT * FROM {}.{}.{}'.format(databaseName, schemaName, tableName)
        string += ' ORDER BY {}'.format(', '.join(clusterByCols))
        string += '\n)'
        return string

    def drawSqlClusterBy(self, databaseName, schemaName, tableName, clusterByCols):
        string = 'ALTER TABLE {}.{}.{}'.format(databaseName, schemaName, tableName)
        string += ' CLUSTER BY ({})'.format(', '.join(clusterByCols))
        return string

    def drawSqlLog(self, stepGroup, stepName):
        string = 'INSERT INTO {}.{}.{}'.format(MASTER_DATABASE, MASTER_SCHEMA, MASTER_LOAD_LOG)
        string += ' VALUES (\'{}\', \'{}\', CURRENT_TIMESTAMP())'.format(stepGroup, stepName)
        return string

    def drawSqlWipeLog(self):
        return 'TRUNCATE TABLE {}.{}.{}'.format(MASTER_DATABASE, MASTER_SCHEMA, MASTER_LOAD_LOG)

    def drawSqlCreateFailedLoadTable(self, queryIds):
        string = 'CREATE OR REPLACE TABLE {}.{}.{} AS ('.format(MASTER_DATABASE, MASTER_SCHEMA, MASTER_FAILED_LOAD_LOG)
        string += '\n    SELECT'
        string += '\n        $1 AS FILE_NAME'
        string += '\n        ,$2 AS STATUS'
        string += '\n        ,$3 AS ROWS_PARSED'
        string += '\n        ,$4 AS ROWS_LOADED'
        string += '\n        ,$5 AS ERROR_LIMIT'
        string += '\n        ,$6 AS ERRORS_SEEN'
        string += '\n        ,$7 AS FIRST_ERROR'
        string += '\n        ,$8 AS FIRST_ERROR_LINE'
        string += '\n        ,$9 AS FIRST_ERROR_CHARACTER'
        string += '\n        ,$10 AS FIRST_ERROR_COLUMN_NAME'
        string += '\n    FROM ('

        for i in range(len(queryIds) - 1):
            string += '\n        SELECT * FROM TABLE(RESULT_SCAN(\'{}\'))'.format(str(queryIds[i]))
            string += '\n            UNION ALL'

        string += '\n        SELECT * FROM TABLE(RESULT_SCAN(\'{}\'))'.format(str(queryIds[-1]))
        string += '\n    ) AS T'
        string += '\n    WHERE'
        string += '\n        $2 = \'LOAD_FAILED\''
        string += '\n)'
        return string

    def run(self):
        # configure the Loader
        self.setAttributes(readConfig('ConnectionConfig'))

        # configure the entities
        self.setEntities(readConfig('DataConfig'))

        # set the database objects to be created
        self.setObjectsToCreate()

        # assign file formats to each entity
        self.assignFileFormats()

        # connect to the source repo (cloud)
        if self.source == 'S3':
            client = filehelper.S3.getClient(self.awsAccessKeyId, self.awsSecretAccessKey)
        elif self.source == 'GCS':
            client = filehelper.GCS.getClient(self.gcsServiceAccountPath)
        elif self.source == 'AZURE':
            client = filehelper.Azure.getClient(self.azureConnectionString)
        else:
            client = None
            self.bucket = os.path.join(self.bucket, self.prefix)

        # connect to Snowflake
        with snowflake.connector.connect(
            account = self.snowflakeAccountId,
            user = self.snowflakeUsername,
            password = self.snowflakePassword,
            warehouse = self.snowflakeWarehouse
        ) as con:
            with con.cursor() as cur:

                # wipe ELT log
                cur.execute(self.drawSqlWipeLog())

                print('\nSTARTED\n...')
                print('Creating DB Objects')

                # logging
                cur.execute(self.drawSqlLog('START', ''))
                cur.execute(self.drawSqlLog('Creating DB Objects', 'Warehouse'))

                # create worker warehouse
                cur.execute(self.drawSqlCreateWarehouse(WORKER_WAREHOUSE))

                # switch to worker warehouse
                cur.execute(self.drawSqlUseWarehouse(WORKER_WAREHOUSE))

                # logging
                cur.execute(self.drawSqlLog('Creating DB Objects', 'Databases'))

                # create databases
                for objToCreate in self.databasesToCreate:
                    if objToCreate != MASTER_DATABASE:
                        cur.execute(self.drawSqlCreateDatabase(
                            objToCreate
                            ))

                # logging
                cur.execute(self.drawSqlLog('Creating DB Objects', 'Schemas'))

                # create schemas
                for objToCreate in self.schemasToCreate:
                    cur.execute(self.drawSqlCreateSchema(
                        objToCreate[0],
                        objToCreate[1]
                        ))

                # logging
                cur.execute(self.drawSqlLog('Creating DB Objects', 'File Formats'))

                # create file formats
                for objToCreate in self.fileFormatsToCreate:
                    cur.execute(self.drawSqlCreateFileFormat(
                        objToCreate[0],
                        objToCreate[1],
                        objToCreate[2],
                        objToCreate[3],
                        objToCreate[4],
                        self.fileFormatsToCreate[objToCreate]
                        ))

                # logging
                cur.execute(self.drawSqlLog('Creating DB Objects', 'Tables'))

                tablesToCreate = set()

                for entity in sorted(self.entities, key = lambda entity: entity.hasHeader, reverse = True):

                    # get key from full table name
                    key = (
                        entity.databaseName,
                        entity.schemaName,
                        entity.tableName
                        )

                    if key not in tablesToCreate:
                        # get any non-empty file for this entity
                        if self.source == 'S3':
                            file = filehelper.S3.getTopNonEmptyFile(client, self.bucket, self.prefix, entity.filePattern)
                        elif self.source == 'GCS':
                            file = filehelper.GCS.getTopNonEmptyFile(client, self.bucket, self.prefix, entity.filePattern)
                        elif self.source == 'AZURE':
                            file = filehelper.Azure.getTopNonEmptyFile(client, self.bucket, self.prefix, entity.filePattern)
                        else:
                            file = filehelper.Local.getTopNonEmptyFile(self.bucket, entity.filePattern)

                        if file is not None:

                            # update entity
                            entity.hasAtLeastOneNonEmptyFile = True

                            # get headers
                            if self.source == 'LOCAL':
                                headers = filehelper.Local.getHeaderCols(file, entity.delimiter)
                            else:
                                headers = filehelper.Cloud.getHeaderCols(client, file, entity.delimiter)

                            # make dummy headers for entity with no header cols supplied
                            if not entity.hasHeader:
                                headers = ['col {}'.format(i + 1) for i in range(len(headers))]

                            # create table
                            cur.execute(self.drawSqlCreateTable(
                                entity.databaseName,
                                entity.schemaName,
                                entity.tableName,
                                headers
                                ))

                            # add table lookup
                            tablesToCreate.add(key)

                # logging
                cur.execute(self.drawSqlLog('Creating DB Objects', 'Stage'))

                # create stage
                if self.source == 'LOCAL':
                    cur.execute(self.drawSqlCreateInternalStage())
                else:
                    cur.execute(self.drawSqlCreateExternalStage(
                        self.source, self.snowflakeIntegration, self.bucket, self.prefix
                        ))

                print('Putting Files')

                # logging
                cur.execute(self.drawSqlLog('Putting Files', ''))

                if self.source == 'LOCAL':
                    fileTypesToPut = set()

                    for entity in self.entities:
                        if entity.fileType not in fileTypesToPut:

                            # put files into stage with matching file extension
                            cur.execute(self.drawSqlPutFiles(
                                self.bucket, entity.fileType
                                ))

                            # add file extention to lookup
                            fileTypesToPut.add(entity.fileType)

                print('Loading Tables')

                queryIds = []

                for entity in self.entities:
                    if entity.hasAtLeastOneNonEmptyFile:
                        
                        # logging
                        msg = '{}.{}.{} ({})'.format(entity.databaseName, entity.schemaName, entity.tableName, entity.filePattern)
                        cur.execute(self.drawSqlLog('Loading Tables', msg))

                        # load table with all matching files
                        cur.execute_async(self.drawSqlCopyInto(
                            entity.databaseName, entity.schemaName, entity.tableName, entity.filePattern, entity.fileFormatName
                            ))

                        # get sfqid
                        queryIds.append(cur.sfqid)

                # wait for all async ops to complete
                for queryId in queryIds:
                    while con.is_still_running(con.get_query_status(queryId)):
                        sleep(1)

                if len(queryIds) > 0:
                    cur.execute(
                            self.drawSqlCreateFailedLoadTable(queryIds)
                        )

                print('Clustering')

                tablesToCluster = dict()

                # get all tables that need to be clustered
                for entity in self.entities:
                    if (len(entity.clusterByCols) > 0) and entity.hasAtLeastOneNonEmptyFile:
                        key = (
                            entity.databaseName,
                            entity.schemaName,
                            entity.tableName
                            )
                        tablesToCluster[key] = entity.clusterByCols

                queryIds = []

                for key in tablesToCluster:

                    # logging
                    msg = '{}.{}.{} (reordering table)'.format(key[0], key[1], key[2])
                    cur.execute(self.drawSqlLog('Cluster', msg))

                    # rebuild table with correct ordering for the clustering
                    cur.execute_async(self.drawSqlReorderTable(
                            key[0], key[1], key[2], tablesToCluster[key]
                        ))

                    # get sfqid
                    queryIds.append(cur.sfqid)

                # wait for all async ops to complete
                for queryId in queryIds:
                    while con.is_still_running(con.get_query_status(queryId)):
                        sleep(1)

                for key in tablesToCluster:

                    # logging
                    msg = '{}.{}.{} (clustering)'.format(key[0], key[1], key[2])
                    cur.execute(self.drawSqlLog('Cluster', msg))

                    # cluster table
                    cur.execute(self.drawSqlClusterBy(
                            key[0], key[1], key[2], tablesToCluster[key]
                        ))

                print('Cleanup')

                # switch back to default warehouse
                cur.execute(self.drawSqlUseWarehouse(self.snowflakeWarehouse))

                # logging
                cur.execute(self.drawSqlLog('Cleanup', 'Dropping Warehouse'))

                # drop warehouse
                cur.execute(self.drawSqlDropWarehouse(WORKER_WAREHOUSE))

                # logging
                cur.execute(self.drawSqlLog('END', ''))

                print('...\nCOMPLETED\n')



if __name__ == '__main__':
    loader = Loader()
    loader.run()
