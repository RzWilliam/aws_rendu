import json
import os
import boto3
import urllib.request
import urllib.error
from decimal import Decimal
from datetime import datetime

# Initialise DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
table = dynamodb.Table(os.environ['STORAGE_CRYPTOPRICES_NAME'])

def handler(event, context):
    # LA CLE API NE FONCTIONNAIT PAS
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=5&page=1&sparkline=false"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
        
        timestamp = datetime.utcnow().isoformat()

        for crypto in data:
            item = {
                "crypto_id": crypto["id"],
                "timestamp": timestamp,
                "name": crypto["name"],
                "symbol": crypto["symbol"],
                "price": Decimal(str(crypto["current_price"]))
            }
            print(f"Inserting: {item}")
            table.put_item(Item=item)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Inserted successfully"})
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
