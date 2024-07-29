import json
from datetime import datetime
import logging
import asyncio
import time
import aioboto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
stage = "Extraction"
today_date = datetime.today().date()


def lambda_handler(event, context):
    # Create a new event loop for this handler invocation
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(handle_event(event, context))

# Asynchronous Handler Function
async def handle_event(event, context):
    logger.info(f'Event: {event}')
    logger.info(f"correlation_id: {event.get('correlation_id')}")
    chunk_key = event.get('chunk_key')
    correlation_id = event.get('correlation_id')
    tenant_id = event.get('tenant_id')
    tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
    execution_arn = event.get('executionArn')
    filepath = None
    
    if not all([chunk_key, correlation_id, tenant_id, tracking_table_name, execution_arn]):
        raise ValueError('Missing required parameters')
    
    try:
        # Record start time
        start_time = time.time()
        
        # Process the chunk
        async with aioboto3.Session().client('s3', region_name='us-east-1') as s3_client, \
                   aioboto3.Session().client('dynamodb') as dynamodb_client:
            filepath = await process_chunk(s3_client, chunk_key, tenant_id, correlation_id)
            raise Exception("Intentional exception")
            # Record end time
            end_time = time.time()
            
            # Calculate execution time
            execution_time = end_time - start_time
            
            # Log execution time
            logger.info(f"Execution time: {execution_time} seconds")
            logger.info(f"filepath: {filepath}")

            # Create DynamoDB table if it does not exist
            await create_dynamodb_table_if_not_exists(dynamodb_client, tracking_table_name)
            logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
            await insert_into_dynamodb(dynamodb_client, tracking_table_name, correlation_id, stage, "Unknown", "Success", execution_arn)
            
            # Return output
            output = {
                'statusCode': 200,
                'raw_data_file_path': filepath,
                'tenant_id_job': f'{tenant_id}/{today_date}',
                'correlation_id': correlation_id,
                'chunk_key': chunk_key,
                'tenant_id': tenant_id,
                'filepath': filepath
            }
            return output
    
    except Exception as e:
        error_output = {
            'error_message': str(e),
            'tenant_id_job': f'{tenant_id}/{today_date}',
            'correlation_id': correlation_id,
            'tenant_id': tenant_id,
            'raw_data_file_path': filepath,
            'filepath': filepath
        }
        # Create DynamoDB table if it does not exist
        async with aioboto3.Session().client('dynamodb') as dynamodb_client:
            await create_dynamodb_table_if_not_exists(dynamodb_client, tracking_table_name)
            logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
            await insert_into_dynamodb(dynamodb_client, tracking_table_name, correlation_id, stage, "Unknown", "Failed", execution_arn)
        
        raise Exception(json.dumps(error_output))

async def process_chunk(s3_client, chunk_key, tenant_id, correlation_id):
    # Define the source and destination paths
    source_bucket = 'event-driven-msc'
    source_key = chunk_key
    destination_key = f'raw-data/{tenant_id}/{correlation_id}/{chunk_key.split("/")[-1]}'
    
    try:
        # Copy the chunk file from the source to the destination
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        await s3_client.copy_object(CopySource=copy_source, Bucket=source_bucket, Key=destination_key)
        logger.info(f"Successfully copied {source_key} to {destination_key}")
        
    except Exception as e:
        logger.error(f"Error copying chunk file: {str(e)}")
        raise

    return destination_key

async def create_dynamodb_table_if_not_exists(dynamodb_client, table_name):
    response = await dynamodb_client.list_tables()
    existing_tables = response.get('TableNames', [])
    
    if table_name not in existing_tables:
        logger.info(f"Table {table_name} does not exist. Creating...")
        await dynamodb_client.create_table(
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
        await dynamodb_client.get_waiter('table_exists').wait(TableName=table_name)
        logger.info(f"Table {table_name} created.")

async def insert_into_dynamodb(dynamodb_client, table_name, correlation_id, stage, tracking_file, job_status, execution_arn):
    if not correlation_id or not stage:
        raise ValueError(f'Missing required keys: correlation_id={correlation_id}, stage={stage}')
        
    item = {
        'CorrelationId': {'S': correlation_id},
        'Stage': {'S': stage},
        'TrackingFile': {'S': tracking_file},
        'JobStatus': {'S': job_status},
        'arn': {'S': execution_arn},
        'timestamp': {'S': datetime.utcnow().isoformat()}
    }
    logger.info(f"Inserting item into DynamoDB: {item}")
    await dynamodb_client.put_item(TableName=table_name, Item=item)
