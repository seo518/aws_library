import json
import boto3

# Description: Gets a list of preloaded files from a bucket. 
# Inputs:
#   Bucket = string = bucket name in CENTRAL CANADA
# Outputs:
#   List = a list of keys in a bucket
def list_all_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    return(buckets)

def lambda_handler(event, context):
    #b = event['body']
    li = {'buckets':list_all_buckets()}
    return {
        'statusCode': 200,
        'body': json.dumps(li)
    }
