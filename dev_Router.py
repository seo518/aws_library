import json
import ast
import boto3

lambda_client = boto3.client('lambda')

# Description: Returns full name of lambda function to be called on
# Inputs:
#   [] : string : name of function in the lambda function list
# Outputs:
#   string : name exact match of the function name returned
getFullName = lambda lambdaMethodName: [method["FunctionName"] for method in lambda_client.list_functions()["Functions"] if lambdaMethodName in method["FunctionName"]][0]

# Description: abstraction of the boto3 client invoke function. The event parameter is assigned as 'arguments' which would be event['body']
# lambda_function : string : name of lambda function in AWS
# method_name : string : name of function inside the lambda function
# params : dictionary : params fed to invoke the function 
# invoke_type : 3 string options: Event | RequestResponse | DryRun
def invoke(lambda_function, method_name, params, invoke_type):
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

def lambda_handler(event, context):
    # Default DryRun if not requested to be run RequestResponse 
    try:
        invoke_t = event['queryStringParameters']['invoke']
    except KeyError:
        invoke_t = 'DryRun'
    
    #Add dev as prefix
    dev_function = 'dev-'+str(event['queryStringParameters']['functionName'])
    try:
        function_name = getFullName(dev_function)
        
        #Create invoke
        body_content = ast.literal_eval(event['body'])
        r_response = invoke(lambda_function = function_name, method_name = 'lambda_handler', params = body_content, invoke_type = invoke_t)
        return {
            'statusCode': 200,
            'body': json.dumps(r_response)
        }
    except IndexError:
        function_name = "Function Not Found"
        return {
            'statusCode': 404,
            'body': json.dumps(function_name)
        }
