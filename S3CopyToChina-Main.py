import traceback
import boto3
import os
import time
import math
import json
import hashlib
from urllib.parse import unquote_plus

s3client = boto3.client('s3')
lambdaclient = boto3.client('lambda')
ddb = boto3.client('dynamodb')
table_parts = 'S3MPU'
table_result = 'S3MPUResult'

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    eventType = event['Records'][0]['eventName']
    dst_bucket = os.environ['DstBucket']
    credbucket = os.environ['CredBucket']
    credobject = os.environ['CredObject']

    # Read China credential
    response = s3client.get_object(Bucket=credbucket, Key=credobject)
    ak = response['Body']._raw_stream.readline().decode("UTF8").strip('\r\n')
    sk = response['Body']._raw_stream.readline().decode("UTF8")
    s3CNclient = boto3.client('s3', region_name='cn-north-1',
                              aws_access_key_id=ak,
                              aws_secret_access_key=sk)

    if not key.endswith('/'):
        try:
            if 'ObjectRemoved' in eventType:
                print('Deleting global S3 object: ' + bucket + '/' + key)
                s3CNclient.delete_object(Bucket=dst_bucket, Key=key)
                print('Deleted China S3 object: ' + dst_bucket + '/' + key)

            if 'ObjectCreated' in eventType:
                head_response = s3client.head_object(Bucket=bucket, Key=key)
                file_length = head_response['ContentLength']
                if file_length <= 5 * 1024 * 1024:
                    # If file size <= 5MB, download object to Lambda temp directory. Then upload to China bucket.
                    print('Copying global S3 object: ' + bucket + '/' + key)
                    hash_id = hashlib.md5(str([time.time(),bucket,key]).encode('utf-8')).hexdigest()
                    ddb.put_item(TableName='S3SingleResult', 
                            Item={
                                'id':{'S': hash_id},
                                'source_bucket':{'S': bucket},
                                'destination_bucket':{'S': dst_bucket},
                                'key':{'S': key},
                                'complete':{'S':'N'}
                                })

                    event_str1 = {
                        'bucket' : bucket,
                        'key' : key,
                        'dst_bucket' : dst_bucket,
                        'id' : hash_id,
                        'credbucket' : credbucket,
                        'credobject' : credobject
                    }
                    payload_json = json.dumps(event_str1)
                    lambdaclient.invoke(
                        FunctionName='S3CopyToChina-Single',
                        InvocationType='Event',
                        Payload=payload_json
                        )
                    print('Invoke Lambda function to process single object.')
                    
                else:
                    # If file size > 5MB, invoke other Lambda to transfer S3 parts by range in parallel.
                    print('Split object '+bucket+'/'+key+' to parts to process by Lambda.')
                    mpu_response = s3CNclient.create_multipart_upload(Bucket=dst_bucket, Key=key)
                    uploadid = mpu_response['UploadId']
                    part_size = 5 * 1024 * 1024
                    position = 0
                    part_qty = math.ceil(file_length/(part_size))
                    i = 1

                    ddb.put_item(TableName=table_result, 
                        Item={
                            'uploadid':{'S':str(uploadid)},
                            'source_bucket':{'S':str(bucket)},
                            'destination_bucket':{'S':str(dst_bucket)},
                            'key':{'S':str(key)},
                            'part_qty':{'N':str(part_qty)},
                            'part_count':{'N':'0'},
                            'complete':{'S':'N'}
                            })
                    
                    while position < file_length :
                        range_string = 'bytes=' + str(position) + '-' + str(position+part_size-1)
                        event_str = {
                            'bucket' : bucket,
                            'key' : key,
                            'dst_bucket' : dst_bucket,
                            'uploadid' : uploadid,
                            'part' : str(i),
                            'range' : range_string,
                            'credbucket' : credbucket,
                            'credobject' : credobject
                        }
                        payload_json = json.dumps(event_str)
                        lambdaclient.invoke(
                            FunctionName='S3CopyToChina-MPU',
                            InvocationType='Event',
                            Payload=payload_json
                            )
                        position += part_size
                        i += 1
                    
                    print('Invoke ' +str(part_qty)+ ' Lambda functions to process MPU object.')

        except Exception as e:
            print(traceback.format_exc())

    return (bucket, key)
