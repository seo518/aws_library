import json
import ast

def lambda_handler(event, context):
    x = event['body']
    print(x)
    print(type(x))
    print(json.dumps(x))
    print(type(json.dumps(x)))
    return {
        'statusCode': 200,
        'body': json.dumps(x)
    }
