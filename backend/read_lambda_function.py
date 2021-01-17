import json
import boto3
import os
from decimal import Decimal
dynamodb = boto3.resource('dynamodb')
def lambda_handler(event, context):
    table = dynamodb.Table('tweets')
    response = table.scan()
    body = json.dumps(response['Items'], default=handle_decimal_type)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Credentials': 'true',
            'Content-Type': 'application/json'
        },
        'body': body
    }

def handle_decimal_type(obj):
  if isinstance(obj, Decimal):
      if float(obj).is_integer():
         return int(obj)
      else:
         return float(obj)
  raise TypeError
  
  
  
# def lambda_handler(event, context):
#   response = table.scan()
#   body = json.dumps(response['Items'], default=handle_decimal_type)
#   response = s3.put_object(Bucket='s3-testing',
#   Key = 's3-testing.json' ,
#   Body=body,
#   ContentType='application/json')

