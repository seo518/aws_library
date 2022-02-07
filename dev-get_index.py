import json
import boto3

# Description: Gets a list of preloaded files from a bucket. 
# Inputs:
#   Bucket = string = bucket name in CENTRAL CANADA
# Outputs:
#   List = a list of keys in a bucket
def get_Index(bucket):
    s3 = boto3.resource('s3')
    obj = s3.Object('xax-index', in_bucket+'.json')
    body = json.loads(obj.get()['Body'].read())
    body = body['key_list']
    return(body)

def lambda_handler(event, context):
    b = event['body']
    li = get_Index(bucket = b['bucket']])
    return {
        'statusCode': 200,
        'body': json.dumps(li)
    }
