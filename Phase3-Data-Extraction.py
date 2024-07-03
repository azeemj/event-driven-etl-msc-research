import boto3
import json
from datetime import datetime
import logging
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3', region_name='us-east-1')
dynamodb_client = boto3.client('dynamodb')
stage = "Extraction"
#tenant_id = 'tenant_one'
today_date = datetime.today().date()


def lambda_handler(event, context):
    logger.info(f'Event: {event}')
    logger.info(f"correlation_id: {event.get('correlation_id')}")
    chunk_key = event.get('chunk_key')
    correlation_id = event.get('correlation_id')
    tenant_id = event.get('tenant_id')
    tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
    
    try:
        # Record start time
        start_time = time.time()
        
        # Process the chunk
        filepath = process_chunk(chunk_key, tenant_id, correlation_id, tracking_table_name)
        
        # Record end time
        end_time = time.time()
        
        # Calculate execution time
        execution_time = end_time - start_time
        
        # Log execution time
        logger.info(f"Execution time: {execution_time} seconds")
        logger.info(f"filepath: {filepath} filepath")
        # Return output
        output = {
            'statusCode': 200,
            'filepath': filepath,
            'tenant_id_job': f'{tenant_id}/{today_date}',
            'correlation_id': correlation_id,
            'chunk_key': chunk_key
            
        }
        
        return output
    
    except Exception as e:
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

def process_chunk(chunk_key, tenant_id, correlation_id, tracking_table_name):
    # Define the source and destination paths
    source_bucket = 'event-driven-msc'
    source_key = chunk_key
    destination_key = f'raw-data/{tenant_id}/{correlation_id}/{chunk_key.split("/")[-1]}'
    
    try:
        # Copy the chunk file from the source to the destination
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=source_bucket, Key=destination_key)
        logger.info(f"Successfully copied {source_key} to {destination_key}")
    except Exception as e:
        logger.error(f"Error copying chunk file: {str(e)}")
        raise

    # Insert tracking information into DynamoDB
    create_dynamodb_table_if_not_exists(tracking_table_name)
    logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
    insert_into_dynamDB(tracking_table_name, correlation_id, stage, destination_key, "Completed")
    
    return destination_key

def create_dynamodb_table_if_not_exists(table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        logger.info(f"Table {table_name} does not exist. Creating...")
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'CorrelationId', 'KeyType': 'HASH'},
                {'AttributeName': 'Stage', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'CorrelationId', 'AttributeType': 'S'},
                {'AttributeName': 'Stage', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Wait until the table exists
        dynamodb_client.get_waiter('table_exists').wait(TableName=table_name)
        logger.info(f"Table {table_name} created.")

def insert_into_dynamDB(table_name, correlation_id, stage, tracking_file, job_status):
    if not correlation_id or not stage:
        raise ValueError(f'Missing required keys: correlation_id={correlation_id}, stage={stage}')
        
    item = {
        'CorrelationId': {'S': correlation_id},
        'Stage': {'S': stage},
        'TrackingFile': {'S': tracking_file},
        'JobStatus': {'S': job_status}
    }
    logger.info(f"Inserting item into DynamoDB: {item}")
    dynamodb_client.put_item(TableName=table_name, Item=item)
