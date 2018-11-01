import traceback
import boto3
import os
import time
import math
import json
from urllib.parse import unquote_plus

s3client = boto3.client('s3')
s3CredClient = boto3.client('s3')
lambda_client = boto3.client('lambda')
ddb = boto3.client('dynamodb')
table_parts = 'S3MPU'
table_result = 'S3MPUResult'

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    dst_bucket = os.environ['ChinaBucket']
    dst_key = key
    eventType = event['Records'][0]['eventName']

    response = s3CredClient.get_object(Bucket=os.environ['CredBucket'], Key=os.environ['CredObject'])
    ak = response['Body']._raw_stream.readline().decode("UTF8").strip('\r\n')
    secret = response['Body']._raw_stream.readline().decode("UTF8")
    s3CNclient = boto3.client('s3', region_name='cn-north-1',
                              aws_access_key_id=ak,
                              aws_secret_access_key=secret)

    if not key.endswith('/'):
        try:
            split_key = key.split('/')
            file_name = '/tmp/' + split_key[-1]

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
                    s3client.download_file(bucket, key, file_name)
                    s3CNclient.upload_file(file_name, dst_bucket, dst_key)
                    ddb.put_item(TableName=table_result, 
                            Item={
                                'uploadid':{'S':'Single'},
                                'destination_key':{'S':str(dst_key)},
                                'complete':{'S':'Y'}
                                })

                    print('Complete uploading to China S3 object: ' + dst_bucket + '/' + dst_key)
                    if os.path.exists(file_name):
                        os.remove(file_name)
                else:
                    # If file size > 5MB, get object parts by range. Upload all parts to temp s3 bucket.
                    part_bucket = 'pingaws-lambda'
                    print('Split object to parts. Upload to temp bucket: '+part_bucket)
                    mpu_response = s3CNclient.create_multipart_upload(Bucket=dst_bucket, Key=key)
                    uploadid = mpu_response['UploadId']
                    part_size = 5 * 1024 * 1024
                    position = 0
                    part_qty = math.ceil(file_length/(5 * 1024 * 1024))
                    i = 1
                    
                    while position < file_length :
                        range_string = 'bytes=' + str(position) + '-' + str(position+part_size-1)
                        response = s3client.get_object(Bucket=bucket, Key=key, Range=range_string)
                        body = response["Body"]
                        partfile_name = '/tmp/part' + str(i)
                        part_key = uploadid+'/'+key+'_part'+str(i)
                        with open(partfile_name, 'wb') as file:
                            file.write(body.read())
                        s3client.upload_file(partfile_name, part_bucket, part_key)
                        head_response = s3client.head_object(Bucket=part_bucket, Key=part_key)
                        etag = eval(head_response['ETag'])

                        # Store s3 parts and result information in DDB.
                        ddb_time = time.time()
                        ddb.put_item(TableName=table_parts, 
                            Item={
                                'uploadid':{'S':str(uploadid)},
                                'part':{'N':str(i)},                                
                                'source_bucket':{'S':str(bucket)},
                                'source_key':{'S':str(key)},
                                'destination_bucket':{'S':str(dst_bucket)},
                                'etag':{'S':str(etag)},
                                'upload_time':{'S':str(ddb_time)},
                                'part_complete':{'S':'N'}
                                })
                        ddb.put_item(TableName=table_result, 
                            Item={
                                'uploadid':{'S':str(uploadid)},
                                'destination_key':{'S':str(dst_key)},
                                'part_qty':{'N':str(part_qty)},
                                'part_count':{'N':'0'},
                                'complete':{'S':'N'}
                                })
                        if os.path.exists(partfile_name):
                            os.remove(partfile_name)
                        position += part_size
                        i += 1

                    print("All parts are copied to temp bucket "+part_bucket+" successfully.")

        except Exception as e:
            print(traceback.format_exc())

    return (bucket, key)
