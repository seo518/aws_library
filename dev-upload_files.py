import json
import boto3
import base64

# Need client initialization, resource_string = "s3"
def upload_file(full_file_path, bucket_name, client):
    with open(full_file_path, 'rb') as data:
        client.put_object(Body=data, Bucket=bucket_name, Key=full_file_path[len(path):])  

def to_s3_bucket(path, bucket_name):
    client = boto3.client("s3")
    try:
        upload_file(full_file_path = path, bucket_name = bucket_name, client = client)
        
        r = {
            'statusCode': 200,
            'body': json.dumps('passed')
        }
        return(r)
    except Exception, e:
        r = {
            'statusCode': 502,
            'body': json.dumps('Upload Failed. Internal Server Error')
        }
        return(r)
        
# Description: External payload upload code. Only useful when data is outside of the cloud environment. 
# Best practice is to have data flowing directly to an s3 bucket so it doesn't need to be uploaded via code.
# Inputs:
#   payload = string = FULL base64 string
#   bucket = string = a name of a bucket within the aws account
#   file_name = string = name of file
# Output:
#   logical = true or false. False if the file is not uploaded
### Template for POST and GET requests
def lambda_handler(event, context):
    b = event['body']
    
    bucket_name = b['bucket']
    b64_string = b['payload']
    file_name = "/tmp/"+b['filename']
    b64_string = base64.standard_b64decode(b64_string)
    newFile = open(file_name, "wb")
    newFile.write(b64_string)
    
    response = to_s3_bucket(path = file_name, bucket_name = bucket_name)
    
    return response
