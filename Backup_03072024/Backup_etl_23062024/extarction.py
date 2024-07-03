import json
import requests
import boto3
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import time
#import uuid

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


tenant_id = 'tenant_one'
today_date = datetime.today().date()

s3_client = boto3.client('s3', region_name='us-east-1')
dynamodb_client = boto3.client('dynamodb')
stage = "Extraction"
tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'

def lambda_handler(event, context):
    # Generate correlation ID
    #correlation_id = str(uuid.uuid4())

    try:
        # Record start time
        start_time = time.time()
        print('xxxxxxxxxxxx')
        #raise Exception("test exception intentionally")
        # Extract data from web scraping
        filepath = f'raw-data/{tenant_id}/{today_date}.json' #crawl_data(correlation_id)
        
        # Record end time
        end_time = time.time()
        
        # Calculate execution time
        execution_time = end_time - start_time
        
        # Log execution time
        logger.info(f"Execution time: {execution_time} seconds")
        
        # Return output
        output = {
            'statusCode': 200,
            'body': json.dumps({'filepath': filepath, 'tenant_id_job':f'{tenant_id}/{today_date}', 'correlation_id':correlation_id})
        }
        
        return output
    
    except Exception as e:
        print('test')
        error_output = {
            'error_message': str(e),
            'tenant_id_job': f'{tenant_id}/{today_date}',
            'correlation_id': correlation_id
        }
         # Create tracking table if it does not exist
        create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        insert_into_dynamDB(tracking_table_name, correlation_id, stage, "Unknown", "Failed")
        
        raise Exception(json.dumps(error_output))
    
def crawl_data(correlation_id): 
    
    # Search Query
    query = 'UK Economy'
    # Encode special characters in the query
    query_encoded = encode_special_characters(query)
    
    # Construct the URL for web scraping
    url = f"https://news.google.com/search?q={query_encoded}&hl=en-US&gl=US&ceid=US%3Aen"
    
    # Send HTTP request to the URL
    response = requests.get(url)
    
    # Parse HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract articles and links
    articles = soup.find_all('article')
    links = [article.find('a')['href'].replace("./articles/", "https://news.google.com/articles/") for article in articles[:10]]
    
    # Extract news text and split
    news_text = [article.get_text(separator='\n') for article in articles]
    news_text_split = [text.split('\n') for text in news_text]
    
    # Write data to S3
    tracking_file = f'raw-data/{tenant_id}/{today_date}.json'
    s3_client.put_object(Bucket='event-driven-msc', Key=tracking_file, Body=json.dumps({'articles': news_text_split}))

    # Insert tracking information into DynamoDB
    # Create tracking table if it does not exist
    create_dynamodb_table_if_not_exists(tracking_table_name)
    logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
    insert_into_dynamDB(tracking_table_name, correlation_id, stage, tracking_file, "Sucess")
   
    return tracking_file

def encode_special_characters(text):
    # Define special characters mapping
    special_characters = {'&': '%26', '=': '%3D', '+': '%2B', ' ': '%20'}
    
    # Encode special characters in the text
    encoded_text = ''.join(special_characters.get(char, char) for char in text.lower())
    
    return encoded_text


def insert_into_dynamDB(tracking_table_name, correlation_id, stage, tracking_file, status):
    response  = dynamodb_client.put_item(
        TableName=tracking_table_name,
        Item={
            "Key": {"S": correlation_id},
            "Stage": {"S": stage},
            "TrackingInfo": {"S": tracking_file},
            "Timestamp": {"S": datetime.utcnow().isoformat()},
            "Status": {"S": status}
        }
    )
    
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error(f"Failed to insert record into DynamoDB: {response}")
        raise Exception("Failed to insert record into DynamoDB")
        
    return True
    
def create_dynamodb_table_if_not_exists(table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'Key', 'KeyType': 'HASH'},  # Partition key
                {'AttributeName': 'Stage', 'KeyType': 'RANGE'}  # Sort key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'Key', 'AttributeType': 'S'},
                {'AttributeName': 'Stage', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
