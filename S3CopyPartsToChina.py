import traceback
import boto3
import os
from urllib.parse import unquote_plus
from boto3.dynamodb.conditions import Key, Attr

s3client = boto3.client('s3')
s3CredClient = boto3.client('s3')
ddb = boto3.client('dynamodb')
table_parts = 'S3MPU'
table_result = 'S3MPUResult'


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    key_uploadid = key[0:key.find('/')]
    key_part = key[key.find('_part'):][5:]
    tmp_file = '/tmp/' + key[key.find('/') + 1:]
    dst_bucket = os.environ['ChinaBucket']

    # Get China credential from global bucket.
    response = s3CredClient.get_object(Bucket=os.environ['CredBucket'], Key=os.environ['CredObject'])
    ak = response['Body']._raw_stream.readline().decode("UTF8").strip('\r\n')
    secret = response['Body']._raw_stream.readline().decode("UTF8")
    s3CNclient = boto3.client('s3', region_name='cn-north-1',
                              endpoint_url='http://s3.cn-north-1.amazonaws.com.cn',
                              aws_access_key_id=ak,
                              aws_secret_access_key=secret)

    # Download s3 part to Lambda /tmp directory.
    s3client.download_file(bucket, key, tmp_file)

    # Get s3 destination key. Multi-uploads part to China bucket.
    ddb_response = ddb.get_item(TableName=table_result,
                                Key={
                                    "uploadid": {"S": key_uploadid}
                                }
                                )
    dst_key = ddb_response['Item']['destination_key']['S']
    with open(tmp_file, 'rb') as f:
        part = s3CNclient.upload_part(
            Body=f.read(), Bucket=dst_bucket, Key=dst_key, UploadId=key_uploadid, PartNumber=int(key_part))

    print('Complete multi-upload S3 part to China S3 bucket:' + dst_bucket + '/' + dst_key + ' part #:' + key_part)

    # update table 'parts'
    ddb.update_item(TableName=table_parts,
                    Key={
                        "uploadid": {"S": key_uploadid},
                        "part": {"N": key_part}
                    },
                    UpdateExpression="set part_complete = :complete",
                    ExpressionAttributeValues={
                        ":complete": {"S": "Y"}
                    },
                    ReturnValues="UPDATED_NEW"
                    )

    # Get s3 parts count and s3 destination key.
    ddb_response = ddb.get_item(TableName=table_result,
                                Key={
                                    "uploadid": {"S": key_uploadid}
                                }
                                )
    part_count = int(ddb_response['Item']['part_count']['N']) + 1
    part_qty = ddb_response['Item']['part_qty']['N']
    dst_key = ddb_response['Item']['destination_key']['S']

    # update table 'result'
    ddb.update_item(TableName=table_result,
                    Key={
                        "uploadid": {"S": key_uploadid}
                    },
                    UpdateExpression="set part_count = :count",
                    ExpressionAttributeValues={
                        ":count": {"N": str(part_count)}
                    },
                    ReturnValues="UPDATED_NEW"
                    )

    # If count equals parts quantity, initial S3 complete MPU.
    if str(part_count) == str(part_qty):
        parts = []
        j = 0
        response = ddb.query(
            TableName=table_parts,
            KeyConditionExpression="uploadid = :id",
            ExpressionAttributeValues={
                ":id": {"S": key_uploadid}
            }
        )
        items = response['Items']
        for i in items:
            parts.append({"PartNumber": int(items[j]["part"]['N']), "ETag": items[j]["etag"]['S']})
            j += 1
        s3CNclient.complete_multipart_upload(Bucket=dst_bucket, Key=dst_key, UploadId=key_uploadid,
                                             MultipartUpload={"Parts": parts})
        ddb.update_item(TableName=table_result,
                        Key={
                            "uploadid": {"S": key_uploadid}
                        },
                        UpdateExpression="set complete = :complete",
                        ExpressionAttributeValues={
                            ":complete": {"S": "Y"}
                        },
                        ReturnValues="UPDATED_NEW"
                        )

    print("Successfully copied S3 file " + dst_key + " to China bucket " + dst_bucket)

