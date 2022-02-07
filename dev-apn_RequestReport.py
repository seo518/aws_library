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

# Description: Sends an intitial request to create the report which will then return a report_id to be waited on. 
# This function is asynchronous. It must be used with the Router with the invocation type of = "Event"
# Inputs:
#   report_interval = string = https://wiki.xandr.com/display/api/Report+Service
#   columns = list = existing fields based on report_type
#   report_type = string = the type of report = https://wiki.xandr.com/display/api/Report+Service
#   filters = dictionary where the dictionary will list the filters needed based on a column based on the report_type, defaulted to None
#   headers = dictionary = authorization header for APN
# Outputs:
#   ID = string = report id from app nexus. 
def RequestReport(report_interval, columns, report_type, appnexus_token, filters = None):
    if filters is None:
        apn_payload = {
            "report" : {
                "format" : "csv",
                "report_interval" : report_interval,
                "columns" : columns,
                "report_type" : "buyer_untargeted_segment_performance"
            }
        }
    else:
        apn_payload = {
            "report" : {
                "format" : "csv",
                "report_interval" : report_interval,
                "columns": columns,
                "filters" : filters,
                "report_type" : report_type
            }
        }
    headers = {
        "Authorization": appnexus_token,
        "Content-Type" : "application/json"
    }
    print(apn_payload)
    r = request_post(url = os.environ['APN_REPORT_URL'], data = json.dumps(apn_payload), headers = headers)
    print(r)
    return(json.loads(r.data.decode('utf-8')))
    
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

# Description: Send a request to APN to create report. No return output. Can only be called as "event"
# Inputs:
#   report_interval = string = https://wiki.xandr.com/display/api/Report+Service
#   columns = list = existing fields based on report_type
#   report_type = string = the type of report = https://wiki.xandr.com/display/api/Report+Service
#   filters = dictionary where the dictionary will list the filters needed based on a column based on the report_type, defaulted to None
# Outputs:
#   ID = string = report id from app nexus. 
def lambda_handler(event, context):
    b = event['body']['report']
    #b['report_interval']
    # b = {
    #     "format":"csv",
    #     "report_interval": "last_7_days",
    #     "columns": ["month","advertiser_name","advertiser_id","insertion_order_id","insertion_order_name","imps","clicks", "total_convs"],
    #     "filters":[{"insertion_order_id":"880711"}],
    #     "report_type":"network_analytics"
    # }
    
    apn_auth = invoke(lambda_function = "apn_get_auth", method_name = "lambda_handler", params = {}, invoke_type = "RequestResponse")
    
    print(b['columns'])
    print(b['report_type'])
    print(b['filters'])
    if b['filters'] is None:
        report_id = RequestReport(report_interval= b['report_interval'], columns = b['columns'], report_type = b['report_type'], appnexus_token = apn_auth['token'])
    else:
        report_id = RequestReport(report_interval= b['report_interval'], columns = b['columns'], report_type = b['report_type'], filters = b['filters'], appnexus_token = apn_auth['token'])
    print(report_id)
    

    #invoke(lambda_function = "apn_ReportWaiter", method_name = "lambda_handler", params = {'token': apn_auth['token'], 'report_id': report_id['response']['report_id']}, invoke_type = "Event")
    return {
        'statusCode': 202,
        'body': json.dumps(report_id)
    }
