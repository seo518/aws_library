#' author: Miguel Ajon
import json
import urllib3
import os
import boto3 
import ast 

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
    #print(type(payload))
    response = lambda_client.invoke(FunctionName = getFullName(lambda_function), InvocationType = invoke_type, Payload = payload)
    #print(response)
    r = json.loads(response["Payload"].read())
    #print(type(r))
    #print(r)
    if r is b'':
        print(response["StatusCode"])
    else:
        return(r)

def request_post(url, data, headers = None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('POST', url,
                             headers={'Content-Type': 'application/json'},
                             body=data)
        else:
            r = http.request('POST', url,
                             headers=headers,
                             body=data)
    else:
         raise print("Data in post request must be string")
        
    return(r)
    
def oauth2_refresh(endpoint, app, auth_token, type = None):
    d = {
        "client_id" : app['client_id'][0],
        "client_secret" : app['client_secret'][0],
        "grant_type" : "refresh_token",
        "refresh_token" : auth_token['refresh_token'][0]
    }
    print(json.dumps(d))
    print(endpoint)
    req = request_post(url = endpoint, data = json.dumps(d))
    return(req)

# Description: use pre-authenticated token to get a refreshed token. Callable only from router
# Inputs:
#   app_name = string = bidmanager OR dfa or a google product
# Outputs:
#   token = full response token from google authentication scope
def lambda_handler(event, context):
    # Get configs
    app = {"bucket":"xax-configs1","key":"google/APIJson/apiAccessCreds.json"}
    auth_token = {"bucket":"xax-configs1","key":"google/APIJson/google.tokens.json"}
    
    r_response = invoke(lambda_function = "dev-indexed_download_s3_file", method_name = 'lambda_handler', params = app, invoke_type = "RequestResponse")
    app = ast.literal_eval(ast.literal_eval(r_response['body']))
    app = app['Creds']['Google']
    
    r_response = invoke(lambda_function = "dev-indexed_download_s3_file", method_name = 'lambda_handler', params = auth_token, invoke_type = "RequestResponse")
    auth_token = ast.literal_eval(ast.literal_eval(r_response['body']))

    b = event['body']
    app_name = b['app_name']
    
    auth_token = auth_token[app_name] 
    
    token = oauth2_refresh(endpoint = os.environ['access'], app = app, auth_token = auth_token)
    print(token.data.decode("utf-8"))
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': token.data.decode("utf-8")
    }
