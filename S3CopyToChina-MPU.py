import boto3
import os
import time
import json
from urllib.parse import unquote_plus

s3client = boto3.client('s3')
ddb = boto3.client('dynamodb')
table_parts = 'S3MPU'
table_result = 'S3MPUResult'
credbucket = os.environ['CredBucket']
credobject = os.environ['CredObject']

def lambda_handler(event, context):
    bucket = event['bucket']
    key = unquote_plus(event['key'])
    dst_bucket = event['dst_bucket']  
    uploadid = event['uploadid']
    part = event['part']
    range= event['range']
    tmp_file = '/tmp/'+ key

    # Store s3 parts information in DDB.
    start_time = time.time()
    ddb.put_item(TableName=table_parts, 
        Item={
            'uploadid':{'S':str(uploadid)},
            'part':{'N':str(part)},
            'range':{'S':str(range[6:])}, 
            'source_bucket':{'S':str(bucket)},
            'source_key':{'S':str(key)},
            'destination_bucket':{'S':str(dst_bucket)},
            'start_time':{'N':str(start_time)},
            'part_complete':{'S':'N'}
            })
            
    # Get China credential from global bucket. 
    response = s3client.get_object(Bucket=credbucket, Key=credobject)
    ak = response['Body']._raw_stream.readline().decode("UTF8").strip('\r\n')
    sk = response['Body']._raw_stream.readline().decode("UTF8")
    s3CNclient = boto3.client('s3', region_name='cn-north-1', 
                            aws_access_key_id=ak,
                            aws_secret_access_key=sk)
    
    # Download s3 part by range. Upload part to China S3 bucket.
    response = s3client.get_object(Bucket=bucket, Key=key, Range=range)
    part_content = s3CNclient.upload_part(
        Body=response["Body"].read(), Bucket=dst_bucket, Key=key, UploadId=uploadid, PartNumber=int(part))
    etag = eval(part_content['ETag'])
    if os.path.exists(tmp_file):
        os.remove(tmp_file)

    print('Complete upload part to China S3 bucket:'+dst_bucket+'/'+key+' part #:'+str(part)+' Upload id: '+uploadid)
    
    # update table 'parts'
    finish_time = time.time()
    ddb.update_item(TableName=table_parts,
        Key={
            "uploadid": {"S": uploadid},
            "part": {"N": part}
            },
        UpdateExpression="set part_complete = :complete, finish_time = :finish_time, etag = :etag",
        ExpressionAttributeValues={
            ":complete": {"S": "Y"},
            ":finish_time": {'N':str(finish_time)},
            ":etag": {'S':str(etag)}
        },
        ReturnValues="UPDATED_NEW"
        )

    # Calculate completed s3 parts count. 
    response = ddb.query(TableName=table_parts,
        KeyConditionExpression="uploadid = :id",
        ProjectionExpression='part',
        FilterExpression ="part_complete = :part_complete",
        ExpressionAttributeValues={
            ":id": {"S": uploadid},
            ":part_complete": {"S": 'Y'}
        }
        )
    part_count = response['Count']

    response = ddb.update_item(TableName=table_result,
        Key={
            "uploadid": {"S": uploadid}
            },
        UpdateExpression="set part_count = :count",
        ConditionExpression="complete = :c",
        ExpressionAttributeValues={
            ":count": {"N": str(part_count)},
            ":c": {"S": "N"}
        },
        ReturnValues="ALL_NEW"
        )
    part_qty = response['Attributes']['part_qty']['N']
    
    
    #If count equals parts quantity, initial S3 complete MPU.
    if str(part_count) == str(part_qty) :
        parts = []
        j = 0
        response = ddb.query(
            TableName=table_parts,
            KeyConditionExpression="uploadid = :id",
            ExpressionAttributeValues={
                ":id": {"S": uploadid}
            }
            )
        items = response['Items']
        for i in items:
            parts.append({"PartNumber": int(items[j]["part"]['N']), "ETag": items[j]["etag"]['S']})
            j += 1
        s3CNclient.complete_multipart_upload(Bucket=dst_bucket, Key=key, UploadId=uploadid, MultipartUpload={"Parts": parts})

        #Record task status.
        ddb.update_item(TableName=table_result,
        Key={
            "uploadid": {"S": uploadid}
            },
        UpdateExpression="set complete = :complete, complete_time = :ctime",
        ExpressionAttributeValues={
            ":complete": {"S": "Y"},
            ":ctime": {"N": str(time.time())}
        },
        ReturnValues="UPDATED_NEW"
        )
        
        # Delete temp ddb items.
        k = 1
        while k <= int(part_qty):
            ddb.delete_item(
                TableName=table_parts,
                Key={
                    "uploadid": {"S": uploadid},
                    "part": {"N": str(k)}
                }
                )
            k += 1
        
        
        print("Successfully copied whole S3 file "+key+" to China bucket "+dst_bucket)
    
    
    

