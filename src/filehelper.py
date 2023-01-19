import os
import boto3
from google.cloud.storage import Client as GoogleCloudStorageClient
from azure.storage.blob import BlobServiceClient
import smart_open
import re

class Local():
    def getFiles(folder, pattern = ''):
        for root, _, files in os.walk(folder):
            for file in files:
                filepath = os.path.join(root, file)

                if (len(pattern) == 0) or re.match(pattern, filepath):
                    yield filepath

    def readCsvLines(filepath):
        with open(filepath, 'r') as file:
            for line in file:
                yield line.replace('\n', '')

    def getTopNonEmptyFile(folder, pattern = ''):
        for file in Local.getFiles(folder, pattern):
            if not Local.isFileEmpty(file):
                return file

    def isFileEmpty(filepath):
        try:
            for line in Local.readCsvLines(filepath):
                if len(line.replace(' ', '')) > 0:
                    return False
                return True
        except:
            return True

    def getHeaderCols(filepath, delimiter):
        for line in Local.readCsvLines(filepath):
            return line.split(delimiter)

class Cloud():
    def readCsvLines(client, keypath):
        with smart_open.open(keypath, transport_params = dict(client = client)) as file:
            for line in file:
                yield line.replace('\n', '')

    def isFileEmpty(client, keypath):
        try:
            for line in Cloud.readCsvLines(client, keypath):
                if len(line.replace(' ', '')) > 0:
                    return False
                return True
        except:
            return True

    def getHeaderCols(client, keypath, delimiter):
        for line in Cloud.readCsvLines(client, keypath):
            return line.split(delimiter)

class S3():
    def getClient(awsAccessKeyId, awsSecretAccessKey):
        return boto3.Session(
                aws_access_key_id = awsAccessKeyId,
                aws_secret_access_key = awsSecretAccessKey
            ).client('s3')

    def getFiles(client, bucket, prefix = '', pattern = ''):
        if bucket.endswith('/'):
            bucket = bucket[: -1]

        if prefix.endswith('/'):
            prefix = prefix[: -1]

        s3Paginator = client.get_paginator('list_objects_v2')

        for page in s3Paginator.paginate(Bucket = bucket, Prefix = prefix):
            for key in page.get('Contents', ()):
                if (len(pattern) == 0) or (re.match(pattern, key['Key'])):
                    yield 's3://{}/{}'.format(bucket, key['Key'])

    def getTopNonEmptyFile(client, bucket, prefix = '', pattern = ''):
        for key in S3.getFiles(client, bucket, prefix, pattern):
            if not Cloud.isFileEmpty(client, key):
                return key

class GCS():
    def getClient(serviceAccountPath):
        return GoogleCloudStorageClient.from_service_account_json(
                serviceAccountPath
            )

    def getFiles(client, bucket, prefix = '', pattern = ''):
        if bucket.endswith('/'):
            bucket = bucket[: -1]

        if prefix.endswith('/'):
            prefix = prefix[: -1]
            
        bucketObj = client.get_bucket(bucket)
        
        for page in bucketObj.list_blobs(prefix = prefix).pages:
            for blob in page:
                if (len(pattern) == 0) or (re.match(pattern, blob.name)):
                    yield 'gs://{}/{}'.format(bucket, blob.name)

    def getTopNonEmptyFile(client, bucket, prefix = '', pattern = ''):
        for key in GCS.getFiles(client, bucket, prefix, pattern):
            if not Cloud.isFileEmpty(client, key):
                return key

class Azure():
    def getClient(connectionString):
        return BlobServiceClient.from_connection_string(
                connectionString
            )

    def getFiles(client, bucket, prefix = '', pattern = ''):
        if bucket.endswith('/'):
            bucket = bucket[: -1]

        if prefix.endswith('/'):
            prefix = prefix[: -1]

        containerClient = client.get_container_client(prefix)
            
        for blob in containerClient.list_blobs():
            if (len(pattern) == 0) or (re.match(pattern, blob['name'])):
                yield 'azure://{}/{}'.format(prefix, blob['name'])

    def getTopNonEmptyFile(client, bucket, prefix = '', pattern = ''):
        for key in Azure.getFiles(client, bucket, prefix, pattern):
            if not Cloud.isFileEmpty(client, key):
                return key
