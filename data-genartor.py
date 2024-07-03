import json
import boto3
import logging
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define bucket name and file key
BUCKET_NAME = 'event-driven-msc'
FILE_KEY = 'data/200mb_test_data.json'
tenant_id = 'tenant_one'
today_date = datetime.today().date()
s3_client = boto3.client('s3', region_name='us-east-1')

def generate_1mb_data():
    # Base data structure
    data = [
                ["The New York Times", "More", "How the U.K.\u2019s Economy Became So Stagnant", "Yesterday", "Eshe Nelson", "By Eshe Nelson"],
                ["MSN", "More", "Is the UK economy ripe for an investment boom?", "Yesterday"],
                ["Reuters UK", "More", "UK economy slows to a halt in April in bad timing for Sunak", "8 days ago", "William Schomberg & Andy Bruce", "By William Schomberg & Andy Bruce"],
                ["The Guardian", "More", "UK economy flatlines in blow to Rishi Sunak\u2019s hopes of recovery", "8 days ago", "Phillip Inman", "By Phillip Inman"],
                ["BBC.com", "More", "GDP: UK economy fails to grow during wet April", "8 days ago", "Mitchell Labiak", "By Mitchell Labiak"],
                ["Financial Times", "More", "Hollywood on Thames is a prize for the UK economy", "6 days ago", "John Gapper", "By John Gapper"],
                ["CNBC", "More", "UK GDP flatlines as PM Sunak pins election campaign on economy", "8 days ago", "Jenni Reid", "By Jenni Reid"]
            ]
    

    # Determine the required multiplier
    multiplier = (1150 * 200)  # Starting point
    json_data = ""
    while len(json_data.encode('utf-8')) < 1_048_576:  # 1 MB = 1,048,576 bytes
        multiplier += 1
        articles_list = data * multiplier
        json_data = json.dumps(articles_list)
        
    # Return data slightly smaller than 1 MB
    return data * (multiplier - 1)

    #return large_data

def lambda_handler(event, context):
    try:
        # Generate 1 MB data
        data = generate_1mb_data()
        
        # Convert data to JSON string
        json_data = json.dumps(data)
        
        # Upload JSON data to S3
        s3_client.put_object(Bucket=BUCKET_NAME, Key=FILE_KEY, Body=json_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully created and uploaded 1 MB JSON file to S3')
        }

    except Exception as e:
        logger.error(f'Error: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
