import aioboto3
import json
from datetime import datetime
import logging
import uuid
import asyncio

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to create a DynamoDB table
async def create_dynamodb_table(dynamodb, table_name):
    try:
        table = await dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'tenant_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'chunk_key',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'tenant_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'chunk_key',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        await dynamodb.get_waiter('table_exists').wait(TableName=table_name)
        return True
    except dynamodb.exceptions.ResourceInUseException:
        return True
    except Exception as e:
        logger.error(f"Error creating DynamoDB table: {str(e)}")
        return f"Error creating DynamoDB table: {str(e)}"

# Function to insert data into DynamoDB
async def insert_into_dynamodb(dynamodb, tenant_id, chunk_key, number_of_records, correlation_id, filename, stage="initial"):
    table_name = f'SplitDataTracker{tenant_id}'
    
    create_table_response = await create_dynamodb_table(dynamodb, table_name)
    if create_table_response is not True:
        return create_table_response

    try:
        response = await dynamodb.put_item(
            TableName=table_name,
            Item={
                'tenant_id': {'S': tenant_id},
                'chunk_key': {'S': chunk_key},
                'upload_timestamp': {'S': datetime.utcnow().isoformat()},
                'number_of_records': {'N': str(number_of_records)},
                'correlation_id': {'S': correlation_id},
                'filename': {'S': filename},
                'stage': {'S': stage}
            }
        )
        return True
    except Exception as e:
        logger.error(f"Error inserting into DynamoDB: {str(e)}")
        return f"Error inserting into DynamoDB: {str(e)}"

# Function to split the file and upload chunks to S3
async def split_file(s3_client, dynamodb, bucket_name, file_key, tenant_id, chunk_size=1000, correlation_id=None):
    download_path = '/tmp/2mb_data.json'

    try:
        await s3_client.download_file(bucket_name, file_key, download_path)
        logger.info(f"Successfully downloaded {file_key} from bucket {bucket_name} to {download_path}")
    except Exception as e:
        logger.error(f"Error downloading file from S3: {str(e)}")
        return f"Error downloading file from S3: {str(e)}"

    try:
        with open(download_path, 'r') as file:
            data = json.load(file)
        logger.info("Successfully loaded JSON data from downloaded file")
    except Exception as e:
        logger.error(f"Error reading JSON data: {str(e)}")
        return f"Error reading JSON data: {str(e)}"

    if not isinstance(data, list):
        error_msg = "The input data must be a list of records"
        logger.error(error_msg)
        return error_msg

    file_name = file_key.split('/')[-1].split('.')[0]
    base_folder = f'data/spilited_file/{tenant_id}/{correlation_id}'
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    chunk_keys = []

    for i, chunk in enumerate(chunks):
        chunk_key = f'{base_folder}/{file_name}_chunk_{i}.json'
        upload_path = f'/tmp/{file_name}_chunk_{i}.json'
        with open(upload_path, 'w') as chunk_file:
            json.dump(chunk, chunk_file)
        try:
            await s3_client.upload_file(upload_path, bucket_name, chunk_key)
            chunk_keys.append(chunk_key)
            
            insert_response = await insert_into_dynamodb(dynamodb, tenant_id, chunk_key, len(chunk), correlation_id, file_name)
            if insert_response is not True:
                return insert_response
        except Exception as e:
            logger.error(f"Error uploading chunk {chunk_key} to S3: {str(e)}")
            return f"Error uploading chunk {chunk_key} to S3: {str(e)}"

    return chunk_keys

# Lambda handler function
def lambda_handler(event, context):
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(handle_event(event, context))
    return result

async def handle_event(event, context):
    session = aioboto3.Session()
    async with session.client('s3') as s3_client, session.client('dynamodb') as dynamodb:
        bucket_name = event.get('bucket_name', 'event-driven-msc')
        tenant_id = event.get('tenant_id', 'tenant_one')
        file_key = event.get('file_key', f'large-data-set/{tenant_id}/1mb_test_data_chunk_0.json')
        chunk_size = event.get('chunk_size', 200)
        log_group_name = event.get('log_group_name')
        
        logger.info(f"Starting split {event}")
        
        execution_id = context.aws_request_id
        custom_message = {'event': 'Step Function Execution Started - Split', 'tenant_id': tenant_id, 'execution_id': execution_id}
        
        correlation_id = str(uuid.uuid4())
        logger.info(f"Starting split file process for tenant: {tenant_id}, file: {file_key}, chunk_size: {chunk_size}, correlation_id: {correlation_id}")

        # Await the result of split_file function
        chunk_keys = await split_file(s3_client, dynamodb, bucket_name, file_key, tenant_id, chunk_size, correlation_id)

        if isinstance(chunk_keys, list):
            return {
                'statusCode': 200,
                'chunk_keys': chunk_keys,
                'tenant_id': tenant_id,
                'file_key': file_key,
                'correlation_id': correlation_id,
                'log_group_name': log_group_name
            }
        else:
            return {
                'statusCode': 500,
                'tenant_id': tenant_id,
                'errorMessage': chunk_keys
            }
