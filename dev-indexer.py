#' author: Miguel Ajon
import json
import boto3
import ast
import os

lambda_client = boto3.client('lambda')
# Description: abstraction of the boto3 client invoke function. The event parameter is assigned as 'arguments' which would be event['body']
# lambda_function : string : name of lambda function in AWS
# method_name : string : name of function inside the lambda function
# params : dictionary : params fed to invoke the function 
# invoke_type : 3 string options: Event | RequestResponse | DryRun
def invoke(lambda_function, method_name, params, invoke_type):
    
    getFullName = lambda lambdaMethodName: [method["FunctionName"] for method in lambda_client.list_functions()["Functions"] if lambdaMethodName in method["FunctionName"]][0]
    
    payload = json.dumps({"function": method_name, "body": params})
    print(type(payload))
    response = lambda_client.invoke(FunctionName = getFullName(lambda_function), InvocationType = invoke_type, Payload = payload)
    print(response)
    r = json.loads(response["Payload"].read())
    print("printing type r")
    print(type(r))
    print(r)
    if r is b'':
        print(response["StatusCode"])
    else:
        return(r)

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

# Description: List all available buckets in region of lambda function
# Outputs:
#   bucket list = string = list of buckets within lambda function region (CENTRAL CANADA)
def list_all_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]

# Description: writes a json file from dictionary python object
# Inputs:
#   dictionary = dict = this input is strict, it needs to be a dictionary otherwise the function errors
#   file_path = string = file path to where the json will be saved to
def write_asJson(file_path, dictionary):
    if type(dictionary) is dict:
        with open(file_path, 'w') as fp:
            json.dump(dictionary, fp)
    else:
        raise("TypeError: dictionary is not dictionary type data")

# Need client initialization, resource_string = "s3"
def upload_file(full_file_path, bucket_name):
    client = boto3.client("s3")
    with open(full_file_path, 'rb') as data:
        client.put_object(Body=data, Bucket=bucket_name, Key=full_file_path[len(path):])

def lambda_handler(event, context):
    payload = dict(bucket = "xax-configs1", key = "indexer/xax-s3indexerconfig.json")
    r_response = invoke(lambda_function = 'dev-indexed_download_s3_file', method_name = 'lambda_handler', params = payload, invoke_type = 'RequestResponse')
    buckets = json.loads(r_response['body'])
    buckets = ast.literal_eval(buckets)
    buckets = buckets['watch_buckets']
    
    s3 = boto3.client('s3')
    bucket_name = os.environ['BUCKET_NAME']
    for x in buckets:
        print(x)
        response = s3.put_object(Bucket=bucket_name,Key=x+".json",Body=json.dumps(dict(key_list = list_s3_files(x))))
        print(response)
    
    return {
        'statusCode': 202,
    }
