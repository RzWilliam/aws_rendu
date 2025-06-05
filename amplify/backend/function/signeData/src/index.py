import boto3
import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import uuid

# Accès aux ressources AWS
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Variables d'environnement
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
    # Scan de la table DynamoDB
    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.scan()
    items = response.get('Items', [])

    # Nettoyage Decimal → float
    cleaned_items = decimal_to_float(items)

    # Tri des données (par exemple, par timestamp décroissant si le champ existe)
    sorted_items = sorted(cleaned_items, key=lambda x: x.get('timestamp', 0), reverse=True)

    # Génération d’un nom de fichier horodaté
    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"exports/crypto_{timestamp_str}.json"

    # Conversion en JSON
    json_data = json.dumps(sorted_items, indent=2)

    # Envoi dans S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=filename,
        Body=json_data,
        ContentType='application/json'
    )

    # Génération de l’URL pré-signée (valide 1h)
    presigned_url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': S3_BUCKET,
            'Key': filename
        },
        ExpiresIn=3600  # 1 heure
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "presigned_url": presigned_url,
            "filename": filename,
            "exported_at": timestamp_str
        })
    }
