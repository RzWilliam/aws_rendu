import json
import os
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
table = dynamodb.Table(os.environ['STORAGE_USERS_NAME'])

def handler(event, context):
    # Vérifie que la méthode est GET
    if event.get('httpMethod') != 'GET':
        return {
            'statusCode': 405,
            'headers': {'Allow': 'GET'},
            'body': json.dumps({'error': 'Méthode non autorisée. Seule la méthode GET est acceptée.'})
        }

    params = event.get('queryStringParameters') or {}

    user_id = params.get('id')
    email = params.get('email')

    if not user_id and not email:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Fournir id ou email'})
        }

    try:
        if user_id:
            response = table.get_item(Key={'id': user_id})
            item = response.get('Item')

        elif email:
            response = table.query(
                IndexName='email-index',
                KeyConditionExpression=Key('email').eq(email)
            )
            items = response.get('Items')
            item = items[0] if items else None

        if not item:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Utilisateur non trouvé'})
            }

        return {
            'statusCode': 200,
            'body': json.dumps({'user': item})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
