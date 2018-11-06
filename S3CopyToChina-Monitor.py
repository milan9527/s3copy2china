import boto3
import json
import time
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    ddb = boto3.resource('dynamodb')

    # Check if s3 multipart upload id exists.
    uploadids = []
    table = ddb.Table('S3MPUResult')
    response = table.scan(
        FilterExpression=Attr('complete').eq('N')
        )
    j = 0
    for i in response['Items']:
        info = response['Items'][j]
        uploadid = info['uploadid']
        uploadids.append(uploadid)
        j += 1

    # Monitor S3 multi part upload task.
    now_time = time.time()
    table = ddb.Table('S3MPU')
    mpu_response = table.scan(
        FilterExpression=Attr('part_complete').eq('N') & Attr('start_time').lt(Decimal(now_time)-300)
        )
    j = 0
    k = 0
    for i in mpu_response['Items']:
        info = mpu_response['Items'][j]
        uploadid = info['uploadid']
        bucket = info['source_bucket']
        key = info['source_key']
        dst_bucket = info['destination_bucket']
        part = info['part']
        range = 'bytes='+info['range']

        if uploadid in uploadids:
            event_str = {
                'bucket' : bucket,
                'key' : key,
                'dst_bucket' : dst_bucket,
                'uploadid' : uploadid,
                'part' : str(part),
                'range' : range
            }
            payload_json = json.dumps(event_str)
            lambdaclient = boto3.client('lambda')
            lambdaclient.invoke(
                FunctionName='S3CopyToChina-MPU',
                InvocationType='Event',
                Payload=payload_json
                )
            k += 1
        else:
            table.delete_item(
                    Key={
                        'uploadid': uploadid,
                        'part': part
                    }
                )
        j += 1
    print('Invoke '+str(k)+' Lambda to restart timeout tasks for multi-parts object.')
        
    # Monitor S3 single object task.
    now_time = time.time()
    table = ddb.Table('S3Single')
    s_response = table.scan(
        FilterExpression=Attr('complete').eq('N') & Attr('start_time').lt(Decimal(now_time)-300)
        )
    
    j = 0
    for m in s_response['Items']:
        s_info = s_response['Items'][j]
        s_bucket = s_info['source_bucket']
        s_key = s_info['key']
        dst_bucket = s_info['destination_bucket']
        id = s_info['id']

        event_str = {
                        'bucket' : s_bucket,
                        'key' : s_key,
                        'dst_bucket' : dst_bucket,
                        'id' : id
                   }
        payload_json = json.dumps(event_str)
        lambdaclient = boto3.client('lambda')
        lambdaclient.invoke(
            FunctionName='S3CopyToChina-Single',
            InvocationType='Event',
            Payload=payload_json
            )
        j += 1
    print('Invoke '+str(j)+' Lambda to restart timeout tasks for single object.')


    
