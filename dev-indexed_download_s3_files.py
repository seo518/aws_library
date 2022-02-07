import json
import boto3

# Description: Exact match download of a file into string
# Inputs:
#   Bucket = string = bucket name in CENTRAL CANADA
#   key = string = file name that exists in bucket = requires exact match
# Output:
#   file = in string format
def get_File(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    body = str(obj.get()['Body'].read().decode('utf-8'))
    return(body)

def lambda_handler(event, context):
    b = event['body']
    li = get_File(bucket = b['bucket'], key = b['key'])
    return {
        'statusCode': 200,
        'body': json.dumps(li)
    }
