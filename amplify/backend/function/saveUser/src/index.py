import json
import os
import boto3
import uuid

dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
table = dynamodb.Table(os.environ['STORAGE_USERS_NAME'])

def handler(event, context):
    if event['httpMethod'] != 'POST':
        return {
            'statusCode': 405,
            'body': json.dumps({'error': 'Méthode non autorisée'})
        }

    try:
        body = json.loads(event['body'])
    except Exception:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Entrée non valide'})
        }
        
    allowed_fields = {'name', 'email'}
    if set(body.keys()) != allowed_fields:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Seuls les champs "name" et "email" sont autorisés'})
        }

    name = body.get('name')
    email = body.get('email')

    if not name or not email:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Champs manquants'})
        }

    try:
        response = table.query(
            IndexName='email-index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('email').eq(email)
        )
        if response['Items']:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Utilisateur avec cet email existe déjà'})
            }
    except Exception as e:
        print('Erreur requête GSI', e)

    # Ajoute le user
    item = {
        'id': str(uuid.uuid4()),
        'name': name,
        'email': email
    }

    table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Utilisateur créé', 'user': item})
    }
