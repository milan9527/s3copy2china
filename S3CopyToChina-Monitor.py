import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    ddb = boto3.resource('dynamodb')
    
    # Monitor S3 multi part upload task.
    table = ddb.Table('S3MPU')
    response = table.scan(
        FilterExpression=Attr('part_complete').eq('N')
        )
    j = 0
    for i in response['Items']:
        bucket = response['Items'][j]['source_bucket']
        key = response['Items'][j]['source_key']
        dst_bucket = response['Items'][j]['destination_bucket']
        uploadid = response['Items'][j]['uploadid']
        part = response['Items'][j]['part']
        range = 'bytes='+response['Items'][j]['range']
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
        j += 1
    print('Invoke '+str(j)+' Lambda to restart timeout tasks.')
    
    # Monitor S3 single object task.
    table = ddb.Table('S3Single')
    response = table.scan(
        FilterExpression=Attr('complete').eq('N')
        )
    j = 0
    for i in response['Items']:
        bucket = response['Items'][j]['source_bucket']
        key = response['Items'][j]['key']
        event_str = {
                    	'Records': [{
                    	    "eventName": "ObjectCreated:Put",
                    		's3': {
                    			'bucket': {
                    				'name': bucket
                    			},
                    			'object': {
                    				'key': key
                    			}
                    		}
                    	}]
                    }
        payload_json = json.dumps(event_str)
        lambdaclient = boto3.client('lambda')
        lambdaclient.invoke(
            FunctionName='S3CopyToChina-Main',
            InvocationType='Event',
            Payload=payload_json
            )
        j += 1
    print('Invoke '+str(j)+' Lambda to restart timeout tasks.')


    
