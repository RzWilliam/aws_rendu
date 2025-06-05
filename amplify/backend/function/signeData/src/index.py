import boto3
import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import uuid

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

DYNAMODB_TABLE = os.environ['STORAGE_CRYPTOPRICES_NAME']
S3_BUCKET = os.environ['STORAGE_CRYPTOEXPORTBUCKET_BUCKETNAME']

def decimal_to_float(obj):
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def handler(event, context):
    if event.get("httpMethod") and event["httpMethod"] != "GET":
        return {
            "statusCode": 405,
            "body": json.dumps({"message": "Méthode non autorisée"})
        }

    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.scan()
    items = response.get('Items', [])

    cleaned_items = decimal_to_float(items)

    sorted_items = sorted(cleaned_items, key=lambda x: x.get('timestamp', 0), reverse=True)

    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"exports/crypto_{timestamp_str}.json"

    json_data = json.dumps(sorted_items, indent=2)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=filename,
        Body=json_data,
        ContentType='application/json'
    )

    presigned_url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': S3_BUCKET,
            'Key': filename
        },
        ExpiresIn=3600
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "presigned_url": presigned_url,
            "filename": filename,
            "exported_at": timestamp_str
        })
    }
