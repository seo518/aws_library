#' author: Miguel Ajon
import json
import boto3
import ast
import urllib3

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


### Template for POST and GET requests
def lambda_handler(event, context):
    p = {"bucket":"xax-configs1","key":"apn/apnauth.json"}
    auth_url = {"bucket":"xax-configs1","key":"apn/auth_url.json"}
    
    r_response = invoke(lambda_function = "dev-indexed_download_s3_file", method_name = 'lambda_handler', params = auth_url, invoke_type = "RequestResponse")
    auth_url = ast.literal_eval(ast.literal_eval(r_response['body']))
    print(auth_url['auth_url'])
    
    r_response = invoke(lambda_function = "dev-indexed_download_s3_file", method_name = 'lambda_handler', params = p, invoke_type = "RequestResponse")
    
    r_response = ast.literal_eval(ast.literal_eval(r_response['body']))
    print(r_response['auth'])
    
    #a = requests.post(url = auth_url['auth_url'], data = r_response)
    #print(a)
    p_data = request_post(url = auth_url['auth_url'], data = json.dumps(r_response))
    p = ast.literal_eval(p_data.data.decode("utf-8"))
    
    
    response = {
        'statusCode': 200,
        'token': p['response']['token']
    }
    print(response)
    return response


