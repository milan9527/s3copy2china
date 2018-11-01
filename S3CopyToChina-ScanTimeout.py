import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    ddb = boto3.resource('dynamodb')
    table = ddb.Table('S3MPU')
    response = table.scan(
        FilterExpression=Attr('part_complete').eq('N')
        )
    j = 0
    for i in response['Items']:
        bucket = response['Items'][j]['source_bucket']
        key = response['Items'][j]['source_key']
        uploadid = response['Items'][j]['uploadid']
        part = response['Items'][j]['part']
        event_str = {
                    	'Records': [{
                    		's3': {
                    			'bucket': {
                    				'name': 'pingaws-lambda'
                    			},
                    			'object': {
                    				'key': uploadid+'/'+key+'_part'+str(part)
                    			}
                    		}
                    	}]
                    }
        payload_json = json.dumps(event_str)
        lambdaclient = boto3.client('lambda')
        lambdaclient.invoke(
            FunctionName='S3CopyPartsToChina',
            InvocationType='Event',
            Payload=payload_json
            )
        j += 1
    print('Invoke Lambda to restart timeout tasks.')

    
