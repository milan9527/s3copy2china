import boto3
import os
import time
import json
import uuid
from urllib.parse import unquote_plus

s3client = boto3.client('s3')
ddb = boto3.client('dynamodb')
credbucket = os.environ['CredBucket']
credobject = os.environ['CredObject']

def lambda_handler(event, context):
    bucket = event['bucket']
    key = unquote_plus(event['key'])
    id = event['id']
    dst_bucket = event['dst_bucket']  
    file_name = '/tmp/' + str(uuid.uuid3(uuid.NAMESPACE_DNS, key))

    print('Start copying S3 file '+bucket+'/'+key+ ' to China S3 bucket '+dst_bucket)
    
    # Read China credential
    response = s3client.get_object(Bucket=credbucket, Key=credobject)
    ak = response['Body']._raw_stream.readline().decode("UTF8").strip('\r\n')
    sk = response['Body']._raw_stream.readline().decode("UTF8")
    s3CNclient = boto3.client('s3', region_name='cn-north-1',
                              aws_access_key_id=ak,
                              aws_secret_access_key=sk)

    ddb.put_item(TableName='S3Single', 
            Item={
                'id':{'S': id},
                'source_bucket':{'S': bucket},
                'destination_bucket':{'S': dst_bucket},
                'key':{'S': key},
                'complete':{'S': 'N'},
                'start_time':{'N': str(time.time())}
                })

    s3client.download_file(bucket, key, file_name)
    s3CNclient.upload_file(file_name, dst_bucket, key)

    ddb.update_item(TableName='S3SingleResult',
        Key={
            "id": {"S": id}
            },
        UpdateExpression="set complete = :c, complete_time = :ctime",
        ExpressionAttributeValues={
            ":c": {"S": "Y"},
            ":ctime": {"N": str(time.time())}
        },
        ReturnValues="UPDATED_NEW" 
    )

    ddb.delete_item(
        TableName='S3Single',
        Key={
            "id": {"S": id}
        }
        )

    if os.path.exists(file_name):
        os.remove(file_name)
        
    print('Complete copying S3 file '+bucket+'/'+key+ ' to China S3 bucket '+dst_bucket)




