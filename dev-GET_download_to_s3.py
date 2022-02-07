#' author: Miguel Ajon
import json
import boto3
import ast
import urllib3
import os

# Description: abstraction of the boto3 client invoke function. The event parameter is assigned as 'arguments' which would be event['body']
# lambda_function : string : name of lambda function in AWS
# method_name : string : name of function inside the lambda function
# params : dictionary : params fed to invoke the function 
# invoke_type : 3 string options: Event | RequestResponse | DryRun
def invoke(lambda_function, method_name, params, invoke_type):
    lambda_client = boto3.client('lambda')
    
    # Description: Returns full name of lambda function to be called on
    # Inputs:
    #   [] : string : name of function in the lambda function list
    # Outputs:
    #   string : name exact match of the function name returned
    getFullName = lambda lambdaMethodName: [method["FunctionName"] for method in lambda_client.list_functions()["Functions"] if lambdaMethodName in method["FunctionName"]][0]
    
    payload = json.dumps({"function": method_name, "body": params})
    print(type(payload))
    response = lambda_client.invoke(FunctionName = getFullName(lambda_function), InvocationType = invoke_type, Payload = payload)
    print(response)
    r = json.loads(response["Payload"].read())
    print(type(r))
    print(r)
    if r is b'':
        print(response["StatusCode"])
    else:
        return(r)

def request_get(url, headers = None):
    http = urllib3.PoolManager()
    if headers is None:
        r = http.request('GET', url)
    else:
        r = http.request('GET', url, headers = headers)
    return(r)

def save_file(file_name, data):
    f = open(file_name, 'wb')
    f.write(data)
    f.close()
    
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


# Description: This function is gets a link download and then puts the object directly to an S3 bucket. 
#       Callable as a RequestResponse or Event.
#   Inputs:
#       url = string = url of the download location
#       headers = string dictionary
#       bucket = bucket name
#       payload = base64 of the payload
#       ext = string = file extension, .csv, .txt., .whatever
#   Output:
#       string = "True" or "False"
def lambda_handler(event, context):
    b = event['body']
    
    if b['headers'] is not None:
        r = request_get(url = b['url'], headers = b['headers'])
    else:
        r = request_get(url = b['url'])
    
    f_name = '/tmp/tmpfile'+b['report_id']+b['ext']
    save_file(file_name = f_name, data = r.data)
    upload_response = to_s3_bucket(path = f_name, bucket_name = b['bucket'])
    
    upload_response = {
        'report_id': b['report_id'],
        'upload_response': upload_response
    }
    
    return(upload_response)
