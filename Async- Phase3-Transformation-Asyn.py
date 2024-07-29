import aioboto3
import json
import time  # Import the time module
from datetime import datetime
import logging
import asyncio
import pandas as pd
import uuid

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

Bucket = 'event-driven-msc'
stage = "Transformation"
today_date = datetime.today().date()

# Asynchronous Lambda Handler
def lambda_handler(event, context):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(handle_event(event, context))

async def handle_event(event, context):
    logger.info(f'Event: {event}')
    logger.info(f"correlation_id: {event.get('correlation_id')}")
    
    chunk_key = event.get('chunk_key')
    correlation_id = event.get('correlation_id')
    tenant_id = event.get('tenant_id')
    #filename = event.get('filepath')
    execution_arn = event.get('executionArn')
    
    #raw file data 
    raw_data_file_path = event.get('raw_data_file_path', None)
    filename = raw_data_file_path
    tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
    
    if not all([chunk_key, correlation_id, filename, execution_arn]):
        raise ValueError('Required missing parameters')
    
    try:
        # Record start time
        start_time = time.time()
        
        # Copy the S3 data to a backup location
        copy_source = {'Bucket': Bucket, 'Key': filename}
        replaced_key = filename.replace('raw-data', 'backup-data')
        backup_key = f'transformed-data/{replaced_key}'
        
        async with aioboto3.Session().client('s3', region_name='us-east-1') as s3_client:
            await s3_client.copy_object(CopySource=copy_source, Bucket=Bucket, Key=backup_key)
        logger.info(f'Backup of {filename} created at {backup_key}')
        
        # Perform transformation
        processed_data = await transformation(event, filename, correlation_id)
        
        # Create tracking table if it does not exist
        await create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        
        # Insert tracking information into DynamoDB
        await insert_into_dynamodb(tracking_table_name, correlation_id, stage, filename, "Success", execution_arn)
        
        # Return output
        output = {
            'statusCode': 200,
            'tenant_id_job': f'{tenant_id}/{today_date}',
            'correlation_id': correlation_id,
            'chunk_key': chunk_key,
            'processed_data': processed_data,
            'tenant_id': tenant_id,
            'raw_data_file_path': raw_data_file_path
        }
        
        # Record end time
        end_time = time.time()
        # Calculate execution time
        execution_time = end_time - start_time
        # Log execution time
        logger.info(f"Execution time: {execution_time} seconds")
        
        return output
    
    except Exception as e:
        # Log and return error response
        logger.error(f'Error: {e}')
        error_output = {
            'error_message': str(e),
            'tenant_id_job': f'{tenant_id}/{today_date}',
            'correlation_id': correlation_id,
            'tenant_id': tenant_id,
            'raw_data_file_path': raw_data_file_path,
            'processed_data': processed_data
        }
        
        # Create tracking table if it does not exist
        await create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        
        # Insert tracking information into DynamoDB
        await insert_into_dynamodb(tracking_table_name, correlation_id, stage, "Unknown", "Failed", execution_arn)
        
        raise Exception(json.dumps(error_output))

async def transformation(event, filename, correlation_id):
    try:
        async with aioboto3.Session().client('s3', region_name='us-east-1') as s3_client:
            response = await s3_client.get_object(Bucket=Bucket, Key=filename)
            json_content = await response['Body'].read()
        
        data = json.loads(json_content.decode('utf-8'))
        
        news_data = []
        for headline in data:
            if len(headline) > 5:
                news_data.append({
                    'Title': headline[2] if len(headline[2]) > 0 else 'Missing',
                    'Source': headline[0] if len(headline[0]) > 0 else 'Missing',
                    'Time': headline[3] if len(headline[3]) > 0 else 'Missing',
                    'Author': headline[4].split('By ')[-1] if len(headline[4]) > 0 else 'Missing',
                    'Link': headline[5] if len(headline[5]) > 0 else 'Missing',
                    'CorrelationId': correlation_id,
                    'Key': str(uuid.uuid4())
                })
        
        news_df = pd.DataFrame(news_data)
        csv_content = news_df.to_csv(index=False)
        
        csv_key = filename.replace('.json', '.csv')
        replaced_key = csv_key.replace('raw-data', 'processed-data')
        transformed_key = f'transformed-data/{replaced_key}'
        
        async with aioboto3.Session().client('s3', region_name='us-east-1') as s3_client:
            await s3_client.put_object(Bucket=Bucket, Key=transformed_key, Body=csv_content.encode('utf-8'))
        
        logger.info(f'Transformation completed.')
        return transformed_key
    
    except Exception as e:
        logger.error(f'Error during transformation: {e}')
        raise e

async def create_dynamodb_table_if_not_exists(table_name):
    async with aioboto3.Session().client('dynamodb', region_name='us-east-1') as dynamodb_client:
        existing_tables = await dynamodb_client.list_tables()
        if table_name not in existing_tables['TableNames']:
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
            await dynamodb_client.get_waiter('table_exists').wait(TableName=table_name)
            logger.info(f"Table {table_name} created.")

async def insert_into_dynamodb(table_name, correlation_id, stage, tracking_file, status, execution_arn):
    if not correlation_id or not stage:
        raise ValueError(f'Missing required keys: correlation_id={correlation_id}, stage={stage}')
    
    item = {
        "CorrelationId": {"S": correlation_id},
        "Stage": {"S": stage},
        "TrackingInfo": {"S": tracking_file},
        "Status": {"S": status},
        'arn': {'S': execution_arn},
        'timestamp': {'S': datetime.utcnow().isoformat()}
    }
    
    logger.info(f"Inserting item into DynamoDB: {item}")
    
    async with aioboto3.Session().client('dynamodb', region_name='us-east-1') as dynamodb_client:
        await dynamodb_client.put_item(TableName=table_name, Item=item)
