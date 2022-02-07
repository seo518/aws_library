import json
import boto3

# Description: Lists files for a specified bucket
# Inputs:
#   bucket = string = a name of a bucket within the aws account 
# Outputs:
#   keys = string = list of keys within the specified bucket
# Example: 
#   list_s3_files("apnreferencefields")
# WARNING: Do not call buckets with log level data with this function
def list_s3_files(bucket):
    session = boto3.session.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket) 
    all_bucket = bucket.objects.all()
    objs = []
    for b in bucket.objects.all():
        objs.append(b.key)
    return(objs)

### Template for POST and GET requests
def lambda_handler(event, context):
    b = event['body']
    print(b['bucket'])
    li = list_s3_files(b['bucket'])
    
    response = {
        'statusCode': 200,
        'body': json.dumps(dict(keys = li))
    } 
    
    return response
