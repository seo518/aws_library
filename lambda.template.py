import json
import ast

### Template for POST and GET requests
def lambda_handler(event, context):
    if event['httpMethod']=="POST" :
        x = ast.literal_eval(event['body'])
        body = { 'message' : x }
        
        response = {
            'statusCode': 200,
            'body': json.dumps(body)
        } 
    elif event['httpMethod']=="GET" and event['queryStringParameters']['test']:
        x = event['queryStringParameters']['test']
        body = { 'message' : x }
        response = {
            'statusCode': 200,
            'body': json.dumps('NA')
        } 
    return response
