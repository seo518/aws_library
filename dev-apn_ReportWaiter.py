#' author: Miguel Ajon
import json
import boto3
import ast
import urllib3
import os
import time


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
    
def time_elapsed(start):
    import time
    end = time.time()
    return(end - start)

def time_elapsed_limit(start, limit):
    # Limit is in seconds
    if time_elapsed(start  = start) >= limit:
        return(True)
    else:
        return(False)

# Description: A waiter function that breaks and loops over itself asynchronously if 10 minutes has passed. This waits for the appnexus to be ready and will repeat until it's ready.
# Inputs:
#   appnexus_token = string = appnexus authentication token
#   report_id = string = id of the report
# Outputs:
#   No outputs. report_id is sent to another function asynchronously
def ReportWaiter(appnexus_token, report_id):
    headers = {
        "Authorization": appnexus_token,
        "Content-Type" : "application/json"
    }
    start = time.time()
    while(1):
        r = request_get(url = os.environ['APN_ID_REPORT']+report_id, headers = headers)
        r = json.loads(r.data.decode('utf-8'))
        if time_elapsed_limit(start = start, limit = 3600):
            invoke(lambda_function = 'apn_ReportWaiter', method_name = 'lambda_handler', params = {'token': appnexus_token, 'report_id': report_id}, invoke_type = 'Event')
            exit()
        else if r['response']['execution_status'] is not 'ready':
            time.sleep(3)
        else if r['response']['execution_status'] is 'ready':
            payload1 = {
                'headers': headers,
                'url':  os.environ['APN_ID_REPORT']+report_id,
                'ext': '.csv',
                'bucket': 'xax-report-outputs',
                'report_id': report_id
                
            }
            invoke(lambda_function = 'GET_download_to_s3', method_name = 'lambda_handler', params = payload1, invoke_type = 'Event')
            exit()

def lambda_handler(event, context):
    b = event['body']
    ReportWaiter(appnexus_token = b['token'], report_id = b['report_id'])
    
